const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);

// FIX 4: Better pingInterval/pingTimeout to survive network hiccups
const io = new Server(server, {
  cors: { origin: "*" },
  pingInterval: 10000,   // send heartbeat every 10s (was default 25s)
  pingTimeout: 30000,    // wait 30s before declaring dead (was 60s but no pingInterval set)
  connectTimeout: 10000,
  transports: ["websocket", "polling"], // websocket first, polling as fallback
});

app.use(express.static(path.join(__dirname, "public")));

app.get("/join/:sessionId", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ─── In-Memory State ──────────────────────────────────────────────────────────
const sessions = {};
const socketToSession = {};

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
    // FIX 2: Only reset ready for the very first game start.
    // Between rounds we no longer use the ready system — so don't touch p.ready here.
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

// FIX 2: readyCount / nonGMCount / allReady only matter for the very first game
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
function endGame(sessionId, winner = null, reason = null) {
  const session = sessions[sessionId];
  if (!session || session.status !== "in-progress") return;

  clearTimeout(session.timer);
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

  setTimeout(() => {
    if (!sessions[sessionId]) return;
    const s = sessions[sessionId];

    if (s.players[s.gameMasterId]) {
      s.players[s.gameMasterId].hasBeenGM = true;
    }

    const nextId = nextGMId(s);
    if (!nextId) {
      s.status = "game-over";
      const finalScores = getScores(s);
      const champion = finalScores[0];
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

  // ── CREATE SESSION ──────────────────────────────────────────────────────────
  socket.on("create-session", ({ playerName }, cb) => {
    playerName = (playerName || "").trim();
    if (!playerName || playerName.length < 2 || playerName.length > 20) {
      return cb({ error: "Name must be 2–20 characters." });
    }

    const sessionId = generateId();
    sessions[sessionId] = {
      id: sessionId,
      gameMasterId: socket.id,
      originalHostId: socket.id,
      question: null,
      answer: null,
      status: "waiting",
      timer: null,
      winnerId: null,
      gmQueue: [socket.id],
      originalGmQueue: [socket.id],
      roundNumber: 1,
      totalRounds: 1,
      streakPlayerId: null,
      streakCount: 0,
      // FIX 2: track whether the first game has ever started
      firstGameStarted: false,
      players: {},
    };

    sessions[sessionId].players[socket.id] = {
      id: socket.id,
      name: playerName,
      score: 0,
      attempts: 3,
      active: true,
      hasBeenGM: false,
      ready: false,
    };

    socket.join(sessionId);
    socketToSession[socket.id] = sessionId;

    cb({
      success: true,
      sessionId,
      playerId: socket.id,
      isGameMaster: true,
      gameMasterId: socket.id,
      players: getPlayers(sessions[sessionId]),
      roundNumber: 1,
      totalRounds: 1,
      firstGameStarted: false,
    });
  });

  // ── JOIN SESSION ────────────────────────────────────────────────────────────
  socket.on("join-session", ({ playerName, sessionId }, cb) => {
    playerName = (playerName || "").trim();
    sessionId = (sessionId || "").trim().toUpperCase();

    if (!playerName || playerName.length < 2 || playerName.length > 20) {
      return cb({ error: "Name must be 2–20 characters." });
    }
    if (!sessionId) return cb({ error: "Session code is required." });

    const session = sessions[sessionId];
    if (!session) return cb({ error: "Session not found. Check the code." });

    // FIX 1: Block join on ANY status that isn't "waiting".
    // Previously only "in-progress" was blocked, letting "round-over" slip through.
    if (session.status !== "waiting") {
      return cb({ error: "Game already in progress. You cannot join now." });
    }

    const takenNames = Object.values(session.players).map((p) => p.name.toLowerCase());
    if (takenNames.includes(playerName.toLowerCase())) {
      return cb({ error: "Name already taken in this session." });
    }

    session.players[socket.id] = {
      id: socket.id,
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

  // ── PLAYER READY (toggle) — only used before first game ────────────────────
  socket.on("player-ready", (cb) => {
    const sessionId = socketToSession[socket.id];
    const session = sessions[sessionId];

    if (!session) return cb && cb({ error: "Not in a session." });
    if (session.status !== "waiting") return cb && cb({ error: "Game is not in lobby." });
    if (session.gameMasterId === socket.id) return cb && cb({ error: "GM doesn't need to ready up." });

    // FIX 2: ready-up only matters before the first game
    if (session.firstGameStarted) {
      return cb && cb({ error: "Ready system only used before the first game." });
    }

    const player = session.players[socket.id];
    if (!player) return cb && cb({ error: "Player not found." });

    player.ready = !player.ready;

    const rc  = readyCount(session);
    const ngc = nonGMCount(session);

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
  socket.on("set-question", ({ question, answer }, cb) => {
    const sessionId = socketToSession[socket.id];
    const session = sessions[sessionId];

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

    io.to(sessionId).emit("question-set", {
      question:       session.question,
      gameMasterName: session.players[socket.id].name,
    });

    cb({ success: true });
  });

  // ── START GAME ──────────────────────────────────────────────────────────────
  socket.on("start-game", (cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = sessions[sessionId];

    if (!session) return cb({ error: "Not in a session." });
    if (session.gameMasterId !== socket.id) return cb({ error: "Only the game master can start." });
    if (playerCount(session) < 3)           return cb({ error: "Need at least 3 players to start." });
    if (!session.question || !session.answer) return cb({ error: "Set a question and answer first." });
    if (session.status === "in-progress")   return cb({ error: "Game already running." });

    // FIX 2 + FIX 5: Only enforce ready-check before the first game.
    // Between rounds the GM can start as soon as question is set + 3+ players.
    if (!session.firstGameStarted) {
      if (!allReady(session)) {
        const rc  = readyCount(session);
        const ngc = nonGMCount(session);
        return cb({ error: `Not all players are ready yet (${rc}/${ngc}).` });
      }
    }

    session.status           = "in-progress";
    session.firstGameStarted = true;
    session.totalRounds      = playerCount(session);
    resetAttempts(session);

    io.to(sessionId).emit("game-started", {
      question:     session.question,
      duration:     60,
      players:      getPlayers(session),
      roundNumber:  session.roundNumber,
      totalRounds:  session.totalRounds,
    });

    session.timer = setTimeout(() => {
      if (sessions[sessionId] && sessions[sessionId].status === "in-progress") {
        endGame(sessionId, null);
      }
    }, 60000);

    cb({ success: true });
  });

  // ── SUBMIT GUESS ────────────────────────────────────────────────────────────
  socket.on("submit-guess", ({ guess }, cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = sessions[sessionId];

    if (!session) return cb({ error: "Not in a session." });
    if (session.status !== "in-progress") return cb({ error: "Game is not running." });
    if (session.gameMasterId === socket.id) return cb({ error: "Game master cannot guess." });

    const player = session.players[socket.id];
    if (!player) return cb({ error: "Player not found." });
    if (!player.active || player.attempts <= 0) return cb({ error: "No attempts remaining." });

    guess = (guess || "").trim();
    if (!guess)              return cb({ error: "Guess cannot be empty." });
    if (guess.length > 200)  return cb({ error: "Guess is too long." });

    player.attempts--;
    if (player.attempts === 0) player.active = false;

    const isCorrect = guess.toLowerCase() === session.answer;

    io.to(sessionId).emit("player-guessed", {
      playerId:     socket.id,
      playerName:   player.name,
      guess,
      attemptsLeft: player.attempts,
      correct:      isCorrect,
    });

    if (isCorrect) {
      cb({ success: true, correct: true });
      endGame(sessionId, { id: socket.id, name: player.name });
    } else {
      cb({ success: true, correct: false, attemptsLeft: player.attempts });
      if (activePlayers(session).length === 0) {
        endGame(sessionId, null, "All players used their attempts.");
      }
    }
  });

  // ── PLAY AGAIN ─────────────────────────────────────────────────────────────
  socket.on("play-again", (cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = sessions[sessionId];

    if (!session) return cb && cb({ error: "Not in a session." });
    if (session.status !== "game-over") return cb && cb({ error: "Game is not over yet." });

    if (session.players[socket.id]) {
      session.players[socket.id].wantsPlayAgain = true;
    }

    function pickNewGM(s) {
      for (const id of s.originalGmQueue) {
        if (s.players[id]) return id;
      }
      return Object.keys(s.players)[0];
    }

    const newGMId = pickNewGM(session);

    session.status         = "waiting";
    session.gameMasterId   = newGMId;
    session.question       = null;
    session.answer         = null;
    session.winnerId       = null;
    session.roundNumber    = 1;
    session.totalRounds    = Object.keys(session.players).length;
    session.streakPlayerId = null;
    session.streakCount    = 0;
    // FIX 2: reset firstGameStarted so ready system kicks in again for the new game
    session.firstGameStarted = false;

    session.gmQueue = session.originalGmQueue.filter(id => session.players[id]);

    Object.values(session.players).forEach(p => {
      p.score          = 0;
      p.attempts       = 3;
      p.active         = true;
      p.ready          = false;
      p.hasBeenGM      = false;
      p.wantsPlayAgain = false;
    });

    io.to(sessionId).emit("play-again-started", {
      gameMasterId:   newGMId,
      gameMasterName: session.players[newGMId].name,
      players:        getPlayers(session),
      roundNumber:    1,
      totalRounds:    session.totalRounds,
      readyCount:     0,
      nonGMCount:     nonGMCount(session),
      firstGameStarted: false,
    });

    if (cb) cb({ success: true });
  });

  // ── SEND REACTION ───────────────────────────────────────────────────────────
  socket.on("send-reaction", ({ emoji }, cb) => {
    const sessionId = socketToSession[socket.id];
    const session   = sessions[sessionId];
    if (!session) return;
    const player = session.players[socket.id];
    if (!player) return;
    const allowed = ["👀","🔥","💀","😂","🤯","👏"];
    if (!allowed.includes(emoji)) return;
    socket.to(sessionId).emit("reaction", { emoji, playerName: player.name, playerId: socket.id });
    if (cb) cb({ success: true });
  });

  // ── DISCONNECT ──────────────────────────────────────────────────────────────
  socket.on("disconnect", () => {
    const sessionId = socketToSession[socket.id];
    if (!sessionId || !sessions[sessionId]) return;

    const session       = sessions[sessionId];
    const leavingPlayer = session.players[socket.id];
    if (!leavingPlayer) return;

    delete session.players[socket.id];
    delete socketToSession[socket.id];

    session.gmQueue = session.gmQueue.filter((id) => id !== socket.id);

    if (playerCount(session) === 0) {
      clearTimeout(session.timer);
      delete sessions[sessionId];
      return;
    }

    socket.to(sessionId).emit("player-left", {
      playerId:   socket.id,
      playerName: leavingPlayer.name,
      players:    getPlayers(session),
      readyCount: readyCount(session),
      nonGMCount: nonGMCount(session),
    });

    if (session.gameMasterId === socket.id) {
      const newGMId = nextGMId(session) || Object.keys(session.players)[0];
      session.gameMasterId = newGMId;

      if (session.status === "in-progress") {
        clearTimeout(session.timer);
        session.status = "round-over";
        io.to(sessionId).emit("game-timeout", {
          answer: session.answer,
          scores: getScores(session),
          reason: "Game master disconnected.",
        });
        setTimeout(() => {
          if (!sessions[sessionId]) return;
          const s = sessions[sessionId];
          if (s.players[s.gameMasterId]) s.players[s.gameMasterId].hasBeenGM = true;
          const nextId = nextGMId(s);
          if (!nextId) {
            s.status = "game-over";
            io.to(sessionId).emit("game-over", {
              champion: getScores(s)[0] || null,
              scores:   getScores(s),
            });
            return;
          }
          s.gameMasterId = nextId;
          s.question     = null;
          s.answer       = null;
          s.status       = "waiting";
          s.roundNumber++;
          resetAttempts(s);
          io.to(sessionId).emit("new-round", {
            gameMasterId:   nextId,
            gameMasterName: s.players[nextId].name,
            players:        getPlayers(s),
            scores:         getScores(s),
            roundNumber:    s.roundNumber,
            totalRounds:    s.totalRounds,
          });
        }, 4000);
      } else if (session.status === "waiting") {
        io.to(sessionId).emit("new-game-master", {
          gameMasterId:   newGMId,
          gameMasterName: session.players[newGMId].name,
        });
      }
    }
  });
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🎮 GuessWave → http://localhost:${PORT}\n`);
});