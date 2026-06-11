require("dotenv").config();
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const { Redis } = require("@upstash/redis");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*" },
  pingInterval: 10000,
  pingTimeout: 30000,
  connectTimeout: 10000,
  transports: ["websocket", "polling"],
});

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

app.use(express.static(path.join(__dirname, "public")));
app.get("/join/:sessionId", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ─── In-Memory (transient — rebuilt on reconnect) ─────────────────────────────
const socketToSession = {};
const disconnectTimers = {};

// ─── Broadcast live connection count to all connected clients ─────────────────
function broadcastLiveCount() {
  const count = io.engine.clientsCount;
  io.emit("live-count", { count });
}

// ─── Redis Helpers ────────────────────────────────────────────────────────────
const SESSION_TTL = 60 * 60 * 24; // 24 hours

async function getSession(sessionId) {
  if (!sessionId) return null;
  const data = await redis.get(`session:${sessionId}`);
  return data || null;
}

async function saveSession(session) {
  await redis.set(`session:${session.id}`, session, { ex: SESSION_TTL });
}

async function deleteSession(sessionId) {
  await redis.del(`session:${sessionId}`);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function generateId(len = 6) {
  return Math.random().toString(36).substring(2, 2 + len).toUpperCase();
}

function getPlayers(session) {
  return Object.values(session.players).map(
    ({ id, name, score, attempts, active, hasBeenGM, ready }) => ({
      id, name, score, attempts, active, hasBeenGM, ready,
    })
  );
}

function getScores(session) {
  return Object.values(session.players)
    .sort((a, b) => b.score - a.score)
    .map(({ id, name, score }) => ({ id, name, score }));
}

function resetAttempts(session) {
  Object.values(session.players).forEach((p) => {
    p.attempts = 3;
    p.active = true;
  });
}

function playerCount(session) {
  return Object.keys(session.players).length;
}

function activePlayers(session) {
  return Object.values(session.players).filter(
    (p) => p.id !== session.gameMasterId && p.active && p.attempts > 0
  );
}

function nextGMId(session) {
  for (let i = 0; i < session.gmQueue.length; i++) {
    const id = session.gmQueue[i];
    if (session.players[id] && !session.players[id].hasBeenGM) {
      return id;
    }
  }
  return null;
}

function readyCount(session) {
  return Object.values(session.players).filter(
    (p) => p.id !== session.gameMasterId && p.ready
  ).length;
}

function nonGMCount(session) {
  return Object.values(session.players).filter(
    (p) => p.id !== session.gameMasterId
  ).length;
}

function allReady(session) {
  const nonGM = Object.values(session.players).filter(
    (p) => p.id !== session.gameMasterId
  );
  return nonGM.length > 0 && nonGM.every((p) => p.ready);
}

// ─── Core Game Logic ──────────────────────────────────────────────────────────
async function endGame(sessionId, winner = null, reason = null) {
  const session = await getSession(sessionId);
  if (!session || session.status !== "in-progress") return;

  session.status = "round-over";
  session.winnerId = winner ? winner.id : null;

  if (winner) {
    session.players[winner.id].score += 10;
    if (session.streakPlayerId === winner.id) {
      session.streakCount++;
    } else {
      session.streakPlayerId = winner.id;
      session.streakCount = 1;
    }
  } else {
    session.streakPlayerId = null;
    session.streakCount = 0;
  }

  const scores = getScores(session);
  await saveSession(session);

  if (winner) {
    io.to(sessionId).emit("game-won", {
      winnerId: winner.id,
      winnerName: winner.name,
      answer: session.answer,
      scores,
      streakCount: session.streakPlayerId === winner.id ? session.streakCount : 1,
    });
  } else {
    io.to(sessionId).emit("game-timeout", {
      answer: session.answer,
      scores,
      reason: reason || "Time's up!",
    });
  }

  setTimeout(async () => {
    const s = await getSession(sessionId);
    if (!s) return;

    if (s.players[s.gameMasterId]) {
      s.players[s.gameMasterId].hasBeenGM = true;
    }

    const nextId = nextGMId(s);
    if (!nextId) {
      s.status = "game-over";
      const finalScores = getScores(s);
      const champion = finalScores[0];
      await saveSession(s);
      io.to(sessionId).emit("game-over", {
        champion: champion || null,
        scores: finalScores,
      });
      return;
    }

    s.gameMasterId = nextId;
    s.question = null;
    s.answer = null;
    s.status = "waiting";
    s.winnerId = null;
    s.roundNumber++;
    resetAttempts(s);
    await saveSession(s);

    io.to(sessionId).emit("new-round", {
      gameMasterId: nextId,
      gameMasterName: s.players[nextId].name,
      players: getPlayers(s),
      scores: getScores(s),
      roundNumber: s.roundNumber,
      totalRounds: s.totalRounds,
    });
  }, 5000);
}

// ─── Socket Events ────────────────────────────────────────────────────────────
io.on("connection", (socket) => {

  broadcastLiveCount();

  // ── CREATE SESSION ──────────────────────────────────────────────────────────
  socket.on("create-session", async ({ playerName }, cb) => {
    playerName = (playerName || "").trim();
    if (!playerName || playerName.length < 2 || playerName.length > 20) {
      return cb({ error: "Name must be 2–20 characters." });
    }

    const sessionId = generateId();
    const session = {
      id: sessionId,
      gameMasterId: socket.id,
      originalHostId: socket.id,
      question: null,
      answer: null,
      status: "waiting",
      winnerId: null,
      gmQueue: [socket.id],
      originalGmQueue: [socket.id],
      roundNumber: 1,
      totalRounds: 1,
      streakPlayerId: null,
      streakCount: 0,
      firstGameStarted: false,
      players: {},
    };

    session.players[socket.id] = {
      id: socket.id,
      originalId: socket.id,
      name: playerName,
      score: 0,
      attempts: 3,
      active: true,
      hasBeenGM: false,
      ready: false,
    };

    await saveSession(session);
    socket.join(sessionId);
    socketToSession[socket.id] = sessionId;

    cb({
      success: true,
      sessionId,
      playerId: socket.id,
      isGameMaster: true,
      gameMasterId: socket.id,
      players: getPlayers(session),
      roundNumber: 1,
      totalRounds: 1,
      firstGameStarted: false,
    });
  });

  // ── JOIN SESSION ────────────────────────────────────────────────────────────
  socket.on("join-session", async ({ playerName, sessionId }, cb) => {
    playerName = (playerName || "").trim();
    sessionId = (sessionId || "").trim().toUpperCase();

    if (!playerName || playerName.length < 2 || playerName.length > 20) {
      return cb({ error: "Name must be 2–20 characters." });
    }
    if (!sessionId) return cb({ error: "Session code is required." });

    const session = await getSession(sessionId);
    if (!session) return cb({ error: "Session not found. Check the code." });

    if (session.status !== "waiting") {
      return cb({ error: "Game already in progress. You cannot join now." });
    }

    const takenNames = Object.values(session.players).map((p) => p.name.toLowerCase());
    if (takenNames.includes(playerName.toLowerCase())) {
      return cb({ error: "Name already taken in this session." });
    }

    session.players[socket.id] = {
      id: socket.id,
      originalId: socket.id,
      name: playerName,
      score: 0,
      attempts: 3,
      active: true,
      hasBeenGM: false,
      ready: false,
    };

    session.gmQueue.push(socket.id);
    session.originalGmQueue.push(socket.id);
    session.totalRounds = Object.keys(session.players).length;

    await saveSession(session);
    socket.join(sessionId);
    socketToSession[socket.id] = sessionId;

    socket.to(sessionId).emit("player-joined", {
      player: session.players[socket.id],
      players: getPlayers(session),
      totalRounds: session.totalRounds,
      readyCount: readyCount(session),
      nonGMCount: nonGMCount(session),
    });

    cb({
      success: true,
      sessionId,
      playerId: socket.id,
      isGameMaster: false,
      gameMasterId: session.gameMasterId,
      players: getPlayers(session),
      question: session.question,
      roundNumber: session.roundNumber,
      totalRounds: session.totalRounds,
      readyCount: readyCount(session),
      nonGMCount: nonGMCount(session),
      firstGameStarted: session.firstGameStarted,
    });
  });

  // ── REJOIN SESSION ──────────────────────────────────────────────────────────
  socket.on("rejoin-session", async ({ token, sessionId }, cb) => {
    const session = await getSession(sessionId);
    if (!session) return cb && cb({ error: "Session not found." });

    const player = Object.values(session.players).find(p => p.originalId === token);
    if (!player) return cb && cb({ error: "Player not found in session." });

    if (disconnectTimers[token]) {
      clearTimeout(disconnectTimers[token]);
      delete disconnectTimers[token];
    }

    const oldId = player.id;

    delete session.players[oldId];
    delete socketToSession[oldId];

    player.id = socket.id;
    session.players[socket.id] = player;

    session.gmQueue = session.gmQueue.map(id => id === oldId ? socket.id : id);
    session.originalGmQueue = session.originalGmQueue.map(id => id === oldId ? socket.id : id);
    if (session.gameMasterId === oldId) session.gameMasterId = socket.id;

    await saveSession(session);
    socket.join(sessionId);
    socketToSession[socket.id] = sessionId;

    // If game is in progress, include timing info for client to sync timer
    const timingInfo = {};
    if (session.status === "in-progress" && session.roundStartTime) {
      timingInfo.roundStartTime = session.roundStartTime;
      timingInfo.roundDuration = session.roundDuration || 60;
    }

    socket.emit("rejoined", {
      sessionId,
      playerId: socket.id,
      originalId: token,
      isGameMaster: session.gameMasterId === socket.id,
      gameMasterId: session.gameMasterId,
      players: getPlayers(session),
      question: session.question,
      gameStatus: session.status,
      roundNumber: session.roundNumber,
      totalRounds: session.totalRounds,
      attemptsLeft: player.attempts,
      firstGameStarted: session.firstGameStarted,
      readyCount: readyCount(session),
      nonGMCount: nonGMCount(session),
      ...timingInfo,
    });

    socket.to(sessionId).emit("player-reconnected", {
      playerId: socket.id,
      playerName: player.name,
      players: getPlayers(session),
    });

    if (cb) cb({ success: true });
  });

  // ── PLAYER READY ────────────────────────────────────────────────────────────
  socket.on("player-ready", async (cb) => {
    const sessionId = socketToSession[socket.id];
    const session = await getSession(sessionId);

    if (!session) return cb && cb({ error: "Not in a session." });
    if (session.status !== "waiting") return cb && cb({ error: "Game is not in lobby." });
    if (session.gameMasterId === socket.id) return cb && cb({ error: "GM doesn't need to ready up." });
    if (session.firstGameStarted) return cb && cb({ error: "Ready system only used before the first game." });

    const player = session.players[socket.id];
    if (!player) return cb && cb({ error: "Player not found." });

    player.ready = !player.ready;

    const rc  = readyCount(session);
    const ngc = nonGMCount(session);

    await saveSession(session);

    io.to(sessionId).emit("ready-update", {
      playerId:   socket.id,
      playerName: player.name,
      ready:      player.ready,
      readyCount: rc,
      nonGMCount: ngc,
      allReady:   allReady(session),
      players:    getPlayers(session),
    });

    if (cb) cb({ success: true, ready: player.ready });
  });

  // ── SET QUESTION ────────────────────────────────────────────────────────────
  socket.on("set-question", async ({ question, answer }, cb) => {
    const sessionId = socketToSession[socket.id];
    const session = await getSession(sessionId);

    if (!session) return cb({ error: "Not in a session." });
    if (session.gameMasterId !== socket.id) return cb({ error: "Only the game master can do that." });
    if (session.status === "in-progress") return cb({ error: "Game is already running." });

    question = (question || "").trim();
    answer   = (answer   || "").trim();

    if (!question || question.length < 3) return cb({ error: "Question must be at least 3 characters." });
    if (!answer   || answer.length < 1)   return cb({ error: "Answer cannot be empty." });
    if (answer.length > 100)              return cb({ error: "Answer is too long (max 100 characters)." });

    session.question = question;
    session.answer   = answer.toLowerCase();

    await saveSession(session);

    io.to(sessionId).emit("question-set", {
      question:       session.question,
      gameMasterName: session.players[socket.id].name,
    });

    cb({ success: true });
  });

  // ── START GAME ──────────────────────────────────────────────────────────────
  socket.on("start-game", async (cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = await getSession(sessionId);

    if (!session) return cb({ error: "Not in a session." });
    if (session.gameMasterId !== socket.id) return cb({ error: "Only the game master can start." });
    if (playerCount(session) < 3)           return cb({ error: "Need at least 3 players to start." });
    if (!session.question || !session.answer) return cb({ error: "Set a question and answer first." });
    if (session.status === "in-progress")   return cb({ error: "Game already running." });

    if (!session.firstGameStarted) {
      if (!allReady(session)) {
        const rc  = readyCount(session);
        const ngc = nonGMCount(session);
        return cb({ error: `Not all players are ready yet (${rc}/${ngc}).` });
      }
    }

    const ROUND_DURATION = 60;
    const roundStartTime = Date.now();

    session.status           = "in-progress";
    session.firstGameStarted = true;
    session.totalRounds      = playerCount(session);
    session.roundStartTime   = roundStartTime;
    session.roundDuration    = ROUND_DURATION;
    resetAttempts(session);

    await saveSession(session);

    io.to(sessionId).emit("game-started", {
      question:        session.question,
      roundStartTime,
      roundDuration:   ROUND_DURATION,
      players:         getPlayers(session),
      roundNumber:     session.roundNumber,
      totalRounds:     session.totalRounds,
    });

    setTimeout(async () => {
      const s = await getSession(sessionId);
      if (s && s.status === "in-progress") {
        await endGame(sessionId, null);
      }
    }, ROUND_DURATION * 1000);

    cb({ success: true });
  });

  // ── SUBMIT GUESS ────────────────────────────────────────────────────────────
  socket.on("submit-guess", async ({ guess }, cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = await getSession(sessionId);

    if (!session) return cb({ error: "Not in a session." });
    if (session.status !== "in-progress") return cb({ error: "Game is not running." });
    if (session.gameMasterId === socket.id) return cb({ error: "Game master cannot guess." });

    const player = session.players[socket.id];
    if (!player) return cb({ error: "Player not found." });
    if (!player.active || player.attempts <= 0) return cb({ error: "No attempts remaining." });

    guess = (guess || "").trim();
    if (!guess)             return cb({ error: "Guess cannot be empty." });
    if (guess.length > 200) return cb({ error: "Guess is too long." });

    player.attempts--;
    if (player.attempts === 0) player.active = false;

    const isCorrect = guess.toLowerCase() === session.answer;

    await saveSession(session);

    io.to(sessionId).emit("player-guessed", {
      playerId:     socket.id,
      playerName:   player.name,
      guess,
      attemptsLeft: player.attempts,
      correct:      isCorrect,
    });

    if (isCorrect) {
      cb({ success: true, correct: true });
      await endGame(sessionId, { id: socket.id, name: player.name });
    } else {
      cb({ success: true, correct: false, attemptsLeft: player.attempts });
      if (activePlayers(session).length === 0) {
        await endGame(sessionId, null, "All players used their attempts.");
      }
    }
  });

  // ── PLAY AGAIN ─────────────────────────────────────────────────────────────
  socket.on("play-again", async (cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = await getSession(sessionId);

    if (!session) return cb && cb({ error: "Not in a session." });
    if (session.status !== "game-over") return cb && cb({ error: "Game is not over yet." });

    function pickNewGM(s) {
      for (const id of s.originalGmQueue) {
        if (s.players[id]) return id;
      }
      return Object.keys(s.players)[0];
    }

    const newGMId = pickNewGM(session);

    session.status           = "waiting";
    session.gameMasterId     = newGMId;
    session.question         = null;
    session.answer           = null;
    session.winnerId         = null;
    session.roundNumber      = 1;
    session.totalRounds      = Object.keys(session.players).length;
    session.streakPlayerId   = null;
    session.streakCount      = 0;
    session.firstGameStarted = false;
    session.roundStartTime   = null;
    session.roundDuration    = null;
    session.gmQueue          = session.originalGmQueue.filter(id => session.players[id]);

    Object.values(session.players).forEach(p => {
      p.score          = 0;
      p.attempts       = 3;
      p.active         = true;
      p.ready          = false;
      p.hasBeenGM      = false;
      p.wantsPlayAgain = false;
    });

    await saveSession(session);

    io.to(sessionId).emit("play-again-started", {
      gameMasterId:     newGMId,
      gameMasterName:   session.players[newGMId].name,
      players:          getPlayers(session),
      roundNumber:      1,
      totalRounds:      session.totalRounds,
      readyCount:       0,
      nonGMCount:       nonGMCount(session),
      firstGameStarted: false,
    });

    if (cb) cb({ success: true });
  });

  // ── SEND REACTION ───────────────────────────────────────────────────────────
  socket.on("send-reaction", async ({ emoji }, cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = await getSession(sessionId);
    if (!session) return;
    const player = session.players[socket.id];
    if (!player) return;
    const allowed = ["👀","🔥","💀","😂","🤯","👏"];
    if (!allowed.includes(emoji)) return;
    socket.to(sessionId).emit("reaction", { emoji, playerName: player.name, playerId: socket.id });
    if (cb) cb({ success: true });
  });

  // ── DISCONNECT ──────────────────────────────────────────────────────────────
  socket.on("disconnect", async () => {
    broadcastLiveCount();

    const sessionId = socketToSession[socket.id];
    delete socketToSession[socket.id];

    if (!sessionId) return;

    const session = await getSession(sessionId);
    if (!session) return;

    const leavingPlayer = session.players[socket.id];
    if (!leavingPlayer) return;

    const token    = leavingPlayer.originalId;
    const socketId = socket.id;

    disconnectTimers[token] = setTimeout(async () => {
      delete disconnectTimers[token];

      const s = await getSession(sessionId);
      if (!s) return;

      const stillThere = Object.values(s.players).find(p => p.originalId === token);
      if (!stillThere || stillThere.id !== socketId) return;

      const player = s.players[socketId];
      if (!player) return;

      delete s.players[socketId];
      s.gmQueue = s.gmQueue.filter(id => id !== socketId);

      if (playerCount(s) === 0) {
        await deleteSession(sessionId);
        return;
      }

      await saveSession(s);

      socket.to(sessionId).emit("player-left", {
        playerId:   socketId,
        playerName: player.name,
        players:    getPlayers(s),
        readyCount: readyCount(s),
        nonGMCount: nonGMCount(s),
      });

      if (s.gameMasterId === socketId) {
        const newGMId = nextGMId(s) || Object.keys(s.players)[0];
        s.gameMasterId = newGMId;

        if (s.status === "in-progress") {
          s.status = "round-over";
          await saveSession(s);
          io.to(sessionId).emit("game-timeout", {
            answer: s.answer,
            scores: getScores(s),
            reason: "Game master disconnected.",
          });
          setTimeout(async () => {
            const fresh = await getSession(sessionId);
            if (!fresh) return;
            if (fresh.players[fresh.gameMasterId]) fresh.players[fresh.gameMasterId].hasBeenGM = true;
            const nextId = nextGMId(fresh);
            if (!nextId) {
              fresh.status = "game-over";
              await saveSession(fresh);
              io.to(sessionId).emit("game-over", {
                champion: getScores(fresh)[0] || null,
                scores:   getScores(fresh),
              });
              return;
            }
            fresh.gameMasterId = nextId;
            fresh.question     = null;
            fresh.answer       = null;
            fresh.status       = "waiting";
            fresh.roundNumber++;
            resetAttempts(fresh);
            await saveSession(fresh);
            io.to(sessionId).emit("new-round", {
              gameMasterId:   nextId,
              gameMasterName: fresh.players[nextId].name,
              players:        getPlayers(fresh),
              scores:         getScores(fresh),
              roundNumber:    fresh.roundNumber,
              totalRounds:    fresh.totalRounds,
            });
          }, 4000);
        } else if (s.status === "waiting") {
          await saveSession(s);
          io.to(sessionId).emit("new-game-master", {
            gameMasterId:   newGMId,
            gameMasterName: s.players[newGMId].name,
          });
        }
      }
    }, 30000);
  });
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🎮 GuessWave → http://localhost:${PORT}\n`);
});