const express = require('express')
const http = require('http')
const crypto = require('crypto')
const { Server } = require('socket.io')

const app = express()
const server = http.createServer(app)
const io = new Server(server, { cors: { origin: process.env.CLIENT_ORIGIN || '*', methods: ['GET', 'POST'] } })
const rooms = new Map()
const makeCode = () => { let code; do code = `QUIZ${crypto.randomBytes(2).toString('hex').toUpperCase()}`; while (rooms.has(code)); return code }
const safeName = value => String(value || '').trim().slice(0, 32)
const publicRoom = room => ({ roomCode: room.roomCode, roomName: room.roomName, roundNumber: room.roundNumber, phase: room.phase, activationAt: room.activationAt, participants: [...room.participants.values()].map(({ id, name, online, buzz }) => ({ id, name, online, buzz })) })
const broadcast = room => io.to(room.roomCode).emit('room-state', publicRoom(room))

app.get('/health', (_, res) => res.json({ ok: true, rooms: rooms.size }))

io.on('connection', socket => {
  socket.on('create-room', ({ hostName, roomName } = {}, ack = () => {}) => {
    const name = safeName(hostName); if (!name) return ack({ ok: false, error: 'Host name is required.' })
    const room = { roomCode: makeCode(), roomName: safeName(roomName) || 'Live Quiz', hostId: socket.id, roundNumber: 0, phase: 'waiting', activationAt: null, roundId: null, participants: new Map(), responses: new Map() }
    rooms.set(room.roomCode, room); socket.join(room.roomCode); socket.data.role = 'host'; socket.data.roomCode = room.roomCode
    ack({ ok: true, roomCode: room.roomCode, sessionToken: socket.id }); broadcast(room)
  })

  socket.on('join-room', ({ roomCode, participantName } = {}, ack = () => {}) => {
    const room = rooms.get(String(roomCode || '').toUpperCase()); const name = safeName(participantName)
    if (!room) return ack({ ok: false, error: 'Room not found.' }); if (!name) return ack({ ok: false, error: 'Display name is required.' })
    if ([...room.participants.values()].some(p => p.name.toLowerCase() === name.toLowerCase() && p.online)) return ack({ ok: false, error: 'That name is already in use.' })
    const id = crypto.randomUUID(); room.participants.set(id, { id, name, online: true, socketId: socket.id, buzz: null }); socket.join(room.roomCode); socket.data.role = 'participant'; socket.data.roomCode = room.roomCode; socket.data.participantId = id
    ack({ ok: true, roomCode: room.roomCode, participantId: id, sessionToken: id }); broadcast(room)
  })

  socket.on('start-round', ({ roomCode } = {}, ack = () => {}) => {
    const room = rooms.get(roomCode); if (!room || room.hostId !== socket.id) return ack({ ok: false, error: 'Host authorization required.' }); if (room.phase !== 'waiting') return ack({ ok: false, error: 'Round already active.' })
    room.roundNumber += 1; room.roundId = crypto.randomUUID(); room.responses = new Map(); room.participants.forEach(p => { p.buzz = null }); room.activationAt = Date.now() + 2200; room.phase = 'preparing'; broadcast(room); io.to(room.roomCode).emit('round-started', { roundId: room.roundId, roundNumber: room.roundNumber, activationAt: room.activationAt }); ack({ ok: true, activationAt: room.activationAt })
    setTimeout(() => { if (room.phase === 'preparing') { room.phase = 'active'; broadcast(room); io.to(room.roomCode).emit('buzzer-activated', { roundId: room.roundId, activationAt: room.activationAt }) } }, 2200)
  })

  socket.on('submit-buzz', ({ roomCode, participantId, roundId, clientTime } = {}, ack = () => {}) => {
    const room = rooms.get(roomCode); const participant = room?.participants.get(participantId); const now = Date.now()
    if (!room || !participant || participant.socketId !== socket.id) return ack({ ok: false, error: 'Participant session invalid.' })
    if (room.roundId !== roundId || room.phase !== 'active') return ack({ ok: false, error: 'This round is not accepting buzzes.' })
    if (participant.buzz) return ack({ ok: false, error: 'You already buzzed this round.' })
    const reactionTime = Math.max(0, now - room.activationAt); participant.buzz = { reactionTime, receivedAt: now, clientTime }; room.responses.set(participantId, participant.buzz); ack({ ok: true, reactionTime }); broadcast(room); io.to(room.roomCode).emit('buzz-registered', { participantId, name: participant.name, reactionTime })
  })

  socket.on('end-round', ({ roomCode } = {}, ack = () => {}) => { const room = rooms.get(roomCode); if (!room || room.hostId !== socket.id) return ack({ ok: false, error: 'Host authorization required.' }); room.phase = 'waiting'; room.activationAt = null; broadcast(room); io.to(room.roomCode).emit('round-ended'); ack({ ok: true }) })
  socket.on('disconnect', () => { const code = socket.data.roomCode; const room = rooms.get(code); if (!room) return; if (room.hostId === socket.id) { room.phase = 'host-offline'; io.to(code).emit('host-disconnected') } else if (socket.data.participantId) { const p = room.participants.get(socket.data.participantId); if (p) p.online = false; broadcast(room) } })
})

const port = Number(process.env.PORT || 4000)
server.listen(port, () => console.log(`[BUZZR] Socket.IO server listening on ${port}`))
