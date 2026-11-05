const express = require('express')
const http = require('http')
const crypto = require('crypto')
const { Server } = require('socket.io')

const app = express()
const server = http.createServer(app)
const PORT = Number(process.env.PORT || 4000)
const CLIENT_ORIGINS = (process.env.CLIENT_ORIGIN || 'http://localhost:3000').split(',').map(v => v.trim()).filter(Boolean)
const io = new Server(server, {
  cors: { origin: CLIENT_ORIGINS, methods: ['GET', 'POST'], credentials: true },
  pingInterval: 25000,
  pingTimeout: 20000,
})

const rooms = new Map()
const ROOM_TTL_MS = Number(process.env.ROOM_TTL_MS || 3 * 60 * 60 * 1000)
const HOST_GRACE_MS = Number(process.env.HOST_GRACE_MS || 5 * 60 * 1000)
const ACTIVATION_LEAD_MS = Number(process.env.ACTIVATION_LEAD_MS || 2500)
const POINTS = [10, 7, 5]

const randomToken = () => crypto.randomBytes(32).toString('base64url')
const hashToken = token => crypto.createHash('sha256').update(String(token)).digest('hex')
const safeName = value => String(value || '').trim().replace(/[<>\u0000-\u001f]/g, '').slice(0, 32)
const makeCode = () => {
  let code
  do code = `QUIZ${crypto.randomBytes(4).toString('hex').toUpperCase()}`
  while (rooms.has(code))
  return code
}
const sortResponses = responses => [...responses.values()].sort((a, b) => a.reactionTimeMs - b.reactionTimeMs || a.serverReceivedAt - b.serverReceivedAt)
const leaderboardFor = round => round ? sortResponses(round.responses).map((r, index) => ({ ...r, rank: index + 1 })) : []

function roomSnapshot(room) {
  const current = room.currentRound
  const leaderboard = leaderboardFor(current)
  const totals = new Map()
  for (const round of room.roundHistory) {
    leaderboardFor(round).forEach((entry, index) => {
      totals.set(entry.participantId, (totals.get(entry.participantId) || 0) + (POINTS[index] || 2))
    })
  }
  if (current && ['preparing', 'active'].includes(current.status)) leaderboard.forEach((entry, index) => {
    totals.set(entry.participantId, (totals.get(entry.participantId) || 0) + (POINTS[index] || 2))
  })
  const overallLeaderboard = [...totals.entries()].map(([participantId, points]) => ({
    participantId,
    name: room.participants.get(participantId)?.name || 'Former participant',
    points,
  })).sort((a, b) => b.points - a.points)
  return {
    roomCode: room.roomCode,
    roomName: room.roomName,
    hostName: room.hostName,
    hostOnline: Boolean(room.hostSocketId),
    phase: current?.status || 'waiting',
    roundNumber: current?.roundNumber || room.roundNumber,
    activationAt: current?.activationAt ?? null,
    roundId: current?.id ?? null,
    participants: [...room.participants.values()].map(p => ({
      id: p.id, name: p.name, online: p.online,
      buzz: current?.responses.get(p.id) ? {
        reactionTimeMs: current.responses.get(p.id).reactionTimeMs,
        rank: leaderboard.find(x => x.participantId === p.id)?.rank ?? null,
      } : null,
      falseStart: current?.falseStarts.has(p.id) || false,
    })),
    leaderboard,
    overallLeaderboard,
    roundHistory: room.roundHistory.map(r => ({
      id: r.id, roundNumber: r.roundNumber, activationAt: r.activationAt,
      endedAt: r.endedAt, leaderboard: leaderboardFor(r),
    })),
  }
}

function broadcast(room) {
  room.lastActivityAt = Date.now()
  io.to(room.roomCode).emit('room-state', roomSnapshot(room))
}

function ackError(ack, error) {
  if (typeof ack === 'function') ack({ ok: false, error })
}

function isRateLimited(socket, key, limit = 12, windowMs = 10000) {
  const now = Date.now()
  socket.data.rateLimits ||= new Map()
  const values = (socket.data.rateLimits.get(key) || []).filter(t => now - t < windowMs)
  if (values.length >= limit) return true
  values.push(now)
  socket.data.rateLimits.set(key, values)
  return false
}

function getRoomAndHost(socket, roomCode) {
  const room = rooms.get(String(roomCode || '').toUpperCase())
  if (!room || !socket.data.roomCode || socket.data.roomCode !== room.roomCode || socket.data.role !== 'host' || room.hostSocketId !== socket.id) return null
  return room
}

function archiveCurrentRound(room) {
  const round = room.currentRound
  if (!round) return
  if (!round.endedAt) round.endedAt = Date.now()
  round.status = 'ended'
  if (!room.roundHistory.some(item => item.id === round.id)) {
    room.roundHistory.push({
      id: round.id,
      roundNumber: round.roundNumber,
      activationAt: round.activationAt,
      startedAt: round.startedAt,
      endedAt: round.endedAt,
      responses: new Map(round.responses),
    })
  }
}

function startRound(room) {
  room.roundNumber += 1
  const round = {
    id: crypto.randomUUID(),
    roundNumber: room.roundNumber,
    status: 'preparing',
    commandAt: Date.now(),
    activationAt: Date.now() + ACTIVATION_LEAD_MS,
    startedAt: null,
    endedAt: null,
    responses: new Map(),
    falseStarts: new Set(),
    activationTimer: null,
  }
  room.currentRound = round
  round.activationTimer = setTimeout(() => {
    if (room.currentRound?.id !== round.id || round.status !== 'preparing') return
    round.status = 'active'
    round.startedAt = Date.now()
    broadcast(room)
    io.to(room.roomCode).emit('buzzer-activated', { roundId: round.id, activationAt: round.activationAt })
  }, Math.max(0, round.activationAt - Date.now()))
  round.activationTimer.unref?.()
  broadcast(room)
  io.to(room.roomCode).emit('round-started', {
    roundId: round.id,
    roundNumber: round.roundNumber,
    activationAt: round.activationAt,
    serverNow: Date.now(),
  })
  return round
}

app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size, uptime: process.uptime() }))

io.on('connection', socket => {
  socket.on('clock-sync', (_payload, ack) => {
    const serverReceiveAt = Date.now()
    if (typeof ack === 'function') ack({ serverReceiveAt, serverSendAt: Date.now() })
  })

  socket.on('create-room', ({ hostName, roomName } = {}, ack) => {
    if (isRateLimited(socket, 'create-room', 5, 60000)) return ackError(ack, 'Too many room creation attempts. Please wait.')
    const name = safeName(hostName)
    if (!name) return ackError(ack, 'Host name is required.')
    const hostToken = randomToken()
    const room = {
      roomCode: makeCode(), roomName: safeName(roomName) || 'Live Quiz', hostName: name,
      hostTokenHash: hashToken(hostToken), hostSocketId: socket.id, hostGraceTimer: null,
      roundNumber: 0, currentRound: null, roundHistory: [], participants: new Map(),
      createdAt: Date.now(), lastActivityAt: Date.now(),
    }
    rooms.set(room.roomCode, room)
    socket.join(room.roomCode)
    socket.data = { ...socket.data, role: 'host', roomCode: room.roomCode }
    if (typeof ack === 'function') ack({ ok: true, roomCode: room.roomCode, sessionToken: hostToken, room: roomSnapshot(room) })
    broadcast(room)
  })

  socket.on('join-room', ({ roomCode, participantName } = {}, ack) => {
    if (isRateLimited(socket, 'join-room', 20, 60000)) return ackError(ack, 'Too many join attempts. Please wait.')
    const code = String(roomCode || '').trim().toUpperCase()
    const room = rooms.get(code)
    const name = safeName(participantName)
    if (!room) return ackError(ack, 'Room not found or expired.')
    if (!name) return ackError(ack, 'Display name is required.')
    if ([...room.participants.values()].some(p => p.name.toLowerCase() === name.toLowerCase())) return ackError(ack, 'That name is already in use in this room.')
    const token = randomToken()
    const id = crypto.randomUUID()
    room.participants.set(id, { id, name, tokenHash: hashToken(token), socketId: socket.id, online: true, joinedAt: Date.now() })
    socket.join(room.roomCode)
    socket.data = { ...socket.data, role: 'participant', roomCode: room.roomCode, participantId: id }
    if (typeof ack === 'function') ack({ ok: true, roomCode: room.roomCode, participantId: id, sessionToken: token, room: roomSnapshot(room) })
    io.to(room.roomCode).emit('participant-joined', { participantId: id, name })
    broadcast(room)
  })

  socket.on('reconnect-session', ({ roomCode, sessionToken } = {}, ack) => {
    const room = rooms.get(String(roomCode || '').toUpperCase())
    if (!room || typeof sessionToken !== 'string' || sessionToken.length < 32) return ackError(ack, 'Session expired. Please rejoin the room.')
    const tokenHash = hashToken(sessionToken)
    if (room.hostTokenHash === tokenHash) {
      clearTimeout(room.hostGraceTimer)
      room.hostSocketId = socket.id
      socket.join(room.roomCode)
      socket.data = { ...socket.data, role: 'host', roomCode: room.roomCode }
      if (typeof ack === 'function') ack({ ok: true, role: 'host', room: roomSnapshot(room) })
      broadcast(room)
      return
    }
    const participant = [...room.participants.values()].find(p => p.tokenHash === tokenHash)
    if (!participant) return ackError(ack, 'Session expired. Please rejoin the room.')
    participant.socketId = socket.id
    participant.online = true
    socket.join(room.roomCode)
    socket.data = { ...socket.data, role: 'participant', roomCode: room.roomCode, participantId: participant.id }
    if (typeof ack === 'function') ack({ ok: true, role: 'participant', participantId: participant.id, room: roomSnapshot(room) })
    broadcast(room)
  })

  socket.on('start-round', ({ roomCode } = {}, ack) => {
    const room = getRoomAndHost(socket, roomCode)
    if (!room) return ackError(ack, 'Host authorisation required.')
    if (room.currentRound && ['preparing', 'active'].includes(room.currentRound.status)) return ackError(ack, 'A round is already in progress.')
    const round = startRound(room)
    if (typeof ack === 'function') ack({ ok: true, roundId: round.id, activationAt: round.activationAt })
  })

  socket.on('submit-buzz', ({ roomCode, participantId, roundId, clientClickPerf, estimatedServerClickAt, clockOffsetMs, roundTripTimeMs, timingUncertaintyMs } = {}, ack) => {
    const room = rooms.get(String(roomCode || '').toUpperCase())
    const participant = room?.participants.get(participantId)
    const round = room?.currentRound
    if (!room || !participant || participant.socketId !== socket.id || socket.data.participantId !== participantId) return ackError(ack, 'Participant session invalid.')
    if (!round || round.id !== roundId || !['preparing', 'active'].includes(round.status)) return ackError(ack, 'This round is not accepting buzzes.')
    // A busy event loop can run the activation timer a little late. If the
    // participant's synchronised click timestamp is after activationAt, allow
    // the valid click instead of rejecting it just because the timer callback
    // has not yet been scheduled by Node.
    if (round.status === 'preparing' && Date.now() >= round.activationAt) {
      clearTimeout(round.activationTimer)
      round.status = 'active'
      round.startedAt = round.activationAt
      broadcast(room)
    }
    if (round.status !== 'active') return ackError(ack, 'The buzzer has not activated yet.')
    if (round.responses.has(participantId)) return ackError(ack, 'You already buzzed this round.')
    const clickAt = Number(estimatedServerClickAt)
    const clientPerf = Number(clientClickPerf)
    const offset = Number(clockOffsetMs)
    const rtt = Number(roundTripTimeMs)
    const uncertainty = Number(timingUncertaintyMs)
    const receivedAt = Date.now()
    if (![clickAt, clientPerf, offset, rtt, uncertainty].every(Number.isFinite)) return ackError(ack, 'Invalid timing data.')
    if (uncertainty < 0 || uncertainty > 5000 || rtt < 0 || rtt > 30000) return ackError(ack, 'Clock synchronisation is too uncertain. Please reconnect and try again.')
    const reactionTimeMs = clickAt - round.activationAt
    if (reactionTimeMs < 0 || reactionTimeMs > 120000 || clickAt > receivedAt + 500 || clickAt < receivedAt - 30000) return ackError(ack, 'Invalid or stale buzz timestamp.')
    const response = {
      participantId, name: participant.name,
      reactionTimeMs: Number(reactionTimeMs.toFixed(3)),
      clientClickPerf: Number(clientPerf.toFixed(3)),
      estimatedServerClickAt: Number(clickAt.toFixed(3)),
      clockOffsetMs: Number(offset.toFixed(3)),
      roundTripTimeMs: Number(rtt.toFixed(3)),
      timingUncertaintyMs: Number(uncertainty.toFixed(3)),
      serverReceivedAt: receivedAt,
    }
    round.responses.set(participantId, response)
    const leaderboard = leaderboardFor(round)
    if (typeof ack === 'function') ack({ ok: true, reactionTimeMs: response.reactionTimeMs, rank: leaderboard.find(x => x.participantId === participantId)?.rank })
    io.to(room.roomCode).emit('buzz-registered', { participantId, name: participant.name, reactionTimeMs: response.reactionTimeMs })
    io.to(room.roomCode).emit('leaderboard-updated', leaderboard)
    broadcast(room)
  })

  socket.on('report-false-start', ({ roomCode, participantId, roundId } = {}, ack) => {
    const room = rooms.get(String(roomCode || '').toUpperCase())
    const participant = room?.participants.get(participantId)
    const round = room?.currentRound
    if (!room || !participant || participant.socketId !== socket.id || !round || round.id !== roundId || round.status !== 'preparing') return ackError(ack, 'False start could not be recorded.')
    round.falseStarts.add(participantId)
    io.to(room.roomCode).emit('false-start-detected', { participantId, name: participant.name })
    broadcast(room)
    if (typeof ack === 'function') ack({ ok: true })
  })

  socket.on('end-round', ({ roomCode } = {}, ack) => {
    const room = getRoomAndHost(socket, roomCode)
    if (!room) return ackError(ack, 'Host authorisation required.')
    const round = room.currentRound
    if (!round || !['preparing', 'active'].includes(round.status)) return ackError(ack, 'There is no active round to end.')
    clearTimeout(round.activationTimer)
    archiveCurrentRound(room)
    broadcast(room)
    io.to(room.roomCode).emit('round-ended', { roundId: round.id, leaderboard: leaderboardFor(round) })
    if (typeof ack === 'function') ack({ ok: true })
  })

  socket.on('reset-round', ({ roomCode } = {}, ack) => {
    const room = getRoomAndHost(socket, roomCode)
    if (!room) return ackError(ack, 'Host authorisation required.')
    if (room.currentRound?.activationTimer) clearTimeout(room.currentRound.activationTimer)
    room.currentRound = null
    broadcast(room)
    io.to(room.roomCode).emit('round-reset')
    if (typeof ack === 'function') ack({ ok: true })
  })

  socket.on('get-leaderboard', ({ roomCode } = {}, ack) => {
    const room = rooms.get(String(roomCode || '').toUpperCase())
    if (!room || socket.data.roomCode !== room.roomCode) return ackError(ack, 'Room access denied.')
    if (typeof ack === 'function') ack({ ok: true, leaderboard: leaderboardFor(room.currentRound), overallLeaderboard: roomSnapshot(room).overallLeaderboard, roundHistory: roomSnapshot(room).roundHistory })
  })

  socket.on('leave-room', ({ roomCode } = {}, ack) => {
    const room = rooms.get(String(roomCode || '').toUpperCase())
    if (room && socket.data.role === 'participant') {
      const participant = room.participants.get(socket.data.participantId)
      if (participant) { participant.online = false; participant.socketId = null }
      socket.leave(room.roomCode)
      broadcast(room)
    }
    if (typeof ack === 'function') ack({ ok: true })
  })

  socket.on('end-room', ({ roomCode } = {}, ack) => {
    const room = getRoomAndHost(socket, roomCode)
    if (!room) return ackError(ack, 'Host authorisation required.')
    if (room.currentRound?.activationTimer) clearTimeout(room.currentRound.activationTimer)
    io.to(room.roomCode).emit('room-ended')
    rooms.delete(room.roomCode)
    if (typeof ack === 'function') ack({ ok: true })
  })

  socket.on('disconnect', () => {
    const room = rooms.get(socket.data.roomCode)
    if (!room) return
    if (socket.data.role === 'host' && room.hostSocketId === socket.id) {
      room.hostSocketId = null
      clearTimeout(room.hostGraceTimer)
      room.hostGraceTimer = setTimeout(() => {
        if (!room.hostSocketId) {
          if (room.currentRound?.activationTimer) clearTimeout(room.currentRound.activationTimer)
          io.to(room.roomCode).emit('room-ended', { reason: 'Host did not reconnect.' })
          rooms.delete(room.roomCode)
        }
      }, HOST_GRACE_MS)
      room.hostGraceTimer.unref?.()
      io.to(room.roomCode).emit('host-disconnected')
      broadcast(room)
    } else if (socket.data.role === 'participant') {
      const participant = room.participants.get(socket.data.participantId)
      if (participant && participant.socketId === socket.id) {
        participant.online = false
        participant.socketId = null
        io.to(room.roomCode).emit('participant-left', { participantId: participant.id })
        broadcast(room)
      }
    }
  })
})

const cleanup = setInterval(() => {
  const now = Date.now()
  for (const [code, room] of rooms) {
    if (now - room.lastActivityAt > ROOM_TTL_MS) {
      if (room.currentRound?.activationTimer) clearTimeout(room.currentRound.activationTimer)
      rooms.delete(code)
      io.to(code).emit('room-ended', { reason: 'Room expired.' })
    }
  }
}, 60000)
cleanup.unref?.()

server.listen(PORT, () => console.log(`[BUZZR] Socket.IO server listening on ${PORT}`))
