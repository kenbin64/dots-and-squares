// table-server.js  (Dots and Boxes)
// Same setup as FastTrack with friends, built as its own server (Ken, 2026-11-23: "not the same
// socket just the same setup"): one Cloudflare Durable Object per table, its own /dotsandboxes route.
//
// What a table does, and only this:
//   - seats up to six people who joined with the table's code, plus bots the host adds
//   - lets the host start the game
//   - passes every move from one player to the others, and keeps the host's latest game state
//     so a player who reloads or drops out comes straight back to the same board
//
// It keeps everything in memory and writes nothing to storage. When the last player leaves and
// the object goes idle, Cloudflare drops it and the table is gone. The server holds nothing.
//
// The messages, as JSON over one WebSocket per player:
//   player -> table   hello {clientId, name, avatar, color}   join, or come back to the same seat
//                     me {name, avatar, color}            change who you are, before the start
//                                                          (color 0-5 or null; one player per color)
//                     bots {count, level}                 host only, before the start
//                     rules {timeLimit}                   host only, before the start. timeLimit false (the
//                                                          default): no time limit at all, and no bot ever
//                                                          takes a person's seat. true: an idle player is
//                                                          warned, and the host may then hand them to a bot.
//                     start {}                            host only
//                     action {action, payload}            a move, passed to everyone else
//                     state {state}                       host only: the game after each change
//                     chat {payload}                      emoji reactions
//                     replay {first}                      host only, after a win: Play again; first = the
//                                                          winner's seat id, who opens the next round
//   Moves and boards carry {round}; the table drops any from another round.
//   table -> player   welcome {you, table}   table {table}   start {table}   replay {round, first}
//                     action {round, from, action, payload}   state {round, seq, state}   chat {...}   error {code, message}

export const MAX_SEATS = 6;
export const CODE_LETTERS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';   // no I, L, O, 0 or 1: easy to read out
export const CODE_LENGTH = 5;
const MAX_MESSAGE = 512 * 1024;           // a game state is tens of kilobytes; this is plenty
const HOST_GRACE_MS = 20000;              // how long a host may be gone before the next player takes over
const BOT_LEVELS = ['easy', 'normal', 'hard', 'expert'];
const BOT_NAMES = ['Ruby', 'Jasper', 'Mango', 'Pip', 'Juniper'];
const AVATARS = ['🎮', '🦊', '🐢', '🦉', '🐙', '🦁', '🐝', '🐬', '🌵', '🚀', '⭐', '🍀'];

export function makeCode() {
  const bytes = crypto.getRandomValues(new Uint8Array(CODE_LENGTH));
  return Array.from(bytes, b => CODE_LETTERS[b % CODE_LETTERS.length]).join('');
}

export function validCode(code) {
  return typeof code === 'string' && code.length === CODE_LENGTH && [...code].every(c => CODE_LETTERS.includes(c));
}

// A name is up to 16 visible characters; anything else becomes "Player".
export function cleanName(name) {
  const text = String(name || '').replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 16);
  return text || 'Player';
}

function cleanAvatar(avatar) {
  return AVATARS.includes(avatar) ? avatar : '🎮';
}

// A color is a board position, 0-5 (Ken, 2026-11-15); anything else means "no preference".
export function cleanColor(color) {
  return Number.isInteger(color) && color >= 0 && color <= 5 ? color : null;
}

export class DbTable {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.open = false;          // true once someone made this table with a new code
    this.code = '';
    this.started = false;
    this.hostId = null;
    this.humans = [];           // [{id, clientId, name, avatar, socket, leftAt}]
    this.bots = [];             // [{id, name, level}]
    this.rules = { timeLimit: false };   // Ken, 2026-11-20: no time limit unless the host turns it on
    this.seq = 0;
    this.round = 0;             // how many times Play again has restarted this table
    this.lastState = null;      // the host's latest game state, as the JSON text it arrived in
    this.nextId = 1;
    this.hostTimer = null;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/create') {
      if (this.open) return new Response('taken', { status: 409 });
      this.open = true;
      this.code = url.searchParams.get('code') || '';
      return new Response('ok');
    }
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('WebSocket only', { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.listen(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  // ---- One player's socket -------------------------------------------------------------------
  listen(socket) {
    let seat = null;
    socket.addEventListener('message', (event) => {
      if (typeof event.data !== 'string' || event.data.length > MAX_MESSAGE) return;
      let msg; try { msg = JSON.parse(event.data); } catch { return; }
      if (!msg || typeof msg.t !== 'string') return;
      if (msg.t === 'hello') { seat = this.hello(socket, msg); return; }
      if (!seat) return;
      if (msg.t === 'me') this.setMe(seat, msg);
      else if (msg.t === 'bots') this.setBots(seat, msg);
      else if (msg.t === 'rules') this.setRules(seat, msg);
      else if (msg.t === 'start') this.start(seat);
      else if (msg.t === 'action') this.relayAction(seat, msg);
      else if (msg.t === 'state') this.keepState(seat, event.data);
      else if (msg.t === 'chat') this.relayChat(seat, msg);
      else if (msg.t === 'replay') this.replay(seat, msg);
    });
    const gone = () => { if (seat && seat.socket === socket) this.leave(seat); };
    socket.addEventListener('close', gone);
    socket.addEventListener('error', gone);
  }

  hello(socket, msg) {
    if (!this.open) return this.refuse(socket, 'no_table', 'There is no table with that code. Check the code, or start a new table.');
    const clientId = String(msg.clientId || '').slice(0, 64);
    let seat = clientId && this.humans.find(h => h.clientId === clientId);
    if (seat) {
      // Coming back (a reload, or the move from the table page to the game): same seat, new socket.
      if (seat.socket && seat.socket !== socket) { try { seat.socket.close(4000, 'replaced'); } catch { } }
      seat.socket = socket;
      seat.leftAt = 0;
      if (!this.started) { seat.name = cleanName(msg.name); seat.avatar = cleanAvatar(msg.avatar); seat.color = this.freeColor(seat, msg.color); }
    } else {
      if (this.started) return this.refuse(socket, 'started', 'This game has already started.');
      if (this.humans.length + this.bots.length >= MAX_SEATS) {
        if (!this.bots.length) return this.refuse(socket, 'full', 'This table is full.');
        this.bots.pop();   // a person always beats a bot to a seat
      }
      seat = { id: 'p' + this.nextId++, clientId: clientId || crypto.randomUUID(), name: cleanName(msg.name), avatar: cleanAvatar(msg.avatar), color: null, socket, leftAt: 0 };
      seat.color = this.freeColor(seat, msg.color);
      this.humans.push(seat);
      if (!this.hostId) this.hostId = seat.id;
    }
    this.send(socket, { t: 'welcome', you: seat.id, table: this.view() });
    if (this.started && this.lastState) socket.send(this.stateFrame());
    this.broadcast({ t: 'table', table: this.view() });
    return seat;
  }

  // Name, piece and color can change until the game starts. A color someone else holds is
  // refused and the player keeps what they had.
  setMe(seat, msg) {
    if (this.started) return;
    seat.name = cleanName(msg.name);
    seat.avatar = cleanAvatar(msg.avatar);
    const want = cleanColor(msg.color);
    if (want !== null && this.freeColor(seat, want) === null) {
      this.send(seat.socket, { t: 'error', code: 'color_taken', message: 'Someone else has that color. Pick another.' });
    } else {
      seat.color = want;
    }
    this.broadcast({ t: 'table', table: this.view() });
  }

  // The color if no other player holds it, otherwise null.
  freeColor(seat, color) {
    const c = cleanColor(color);
    if (c === null) return null;
    return this.humans.some(h => h !== seat && h.color === c) ? null : c;
  }

  setBots(seat, msg) {
    if (seat.id !== this.hostId || this.started) return;
    const room = MAX_SEATS - this.humans.length;
    const count = Math.max(0, Math.min(room, Number(msg.count) | 0));
    const level = BOT_LEVELS.includes(msg.level) ? msg.level : 'normal';
    this.bots = Array.from({ length: count }, (_, i) => ({ id: 'bot-' + (i + 1), name: 'Bot ' + BOT_NAMES[i], level }));
    this.broadcast({ t: 'table', table: this.view() });
  }

  // The table's rules, set by the host before the start and the same on every screen.
  setRules(seat, msg) {
    if (seat.id !== this.hostId || this.started) return;
    this.rules = { timeLimit: msg.timeLimit === true };
    this.broadcast({ t: 'table', table: this.view() });
  }

  start(seat) {
    if (seat.id !== this.hostId || this.started) return;
    if (this.humans.length + this.bots.length < 2) return this.send(seat.socket, { t: 'error', code: 'few', message: 'A game needs at least two players. Add a bot or wait for a friend.' });
    this.started = true;
    this.broadcast({ t: 'start', table: this.view() });
  }

  // Play again (Ken, 2026-11-11): the host, once the game is over, starts the next round at the same
  // table with the same seats. The old game state is dropped and every screen reloads into round N,
  // which deals a new deck (the round number goes into the seed). Still only the people already seated.
  // The host names the round's winner ({first: seat id}); every screen opens the next round on that seat
  // (Ken, 2026-11-20: "winner goes first"). Only a seated id counts; anything else is dropped.
  replay(seat, msg) {
    if (seat.id !== this.hostId || !this.started) return;
    const ids = [...this.humans.map(h => h.id), ...this.bots.map(b => b.id)];
    const first = msg && ids.includes(msg.first) ? msg.first : null;
    this.round += 1;
    this.lastState = null;
    this.seq = 0;
    this.broadcast({ t: 'replay', round: this.round, first });
  }

  // Moves and boards carry the round they belong to; anything from another round (a late message from the
  // round just finished, sent in the moment before that page reloaded) is dropped (Ken, 2026-11-20).
  relayAction(seat, msg) {
    if (!this.started || typeof msg.action !== 'string' || (msg.round | 0) !== this.round) return;
    this.broadcast({ t: 'action', round: this.round, from: seat.id, action: msg.action, payload: msg.payload || {} }, seat);
  }

  // Only the host's game state counts: the host runs the bots and the turn order.
  keepState(seat, text) {
    if (!this.started || seat.id !== this.hostId) return;
    let msg; try { msg = JSON.parse(text); } catch { return; }
    if (!msg.state || typeof msg.state !== 'object' || (msg.round | 0) !== this.round) return;
    this.seq += 1;
    this.lastState = JSON.stringify(msg.state);
    const frame = this.stateFrame();
    for (const h of this.humans) if (h !== seat && h.socket) { try { h.socket.send(frame); } catch { } }
  }

  relayChat(seat, msg) {
    const payload = msg.payload && typeof msg.payload === 'object' ? msg.payload : {};
    const text = JSON.stringify(payload);
    if (text.length > 2000) return;
    this.broadcast({ t: 'chat', from: seat.id, username: seat.name, ...payload }, seat);
  }

  leave(seat) {
    seat.socket = null;
    seat.leftAt = Date.now();
    if (!this.started) {
      // Before the start a seat that goes is simply given up (a reload comes straight back in).
      setTimeout(() => {
        if (seat.socket || this.started) return;
        this.humans = this.humans.filter(h => h !== seat);
        if (this.hostId === seat.id) this.hostId = this.humans[0] ? this.humans[0].id : null;
        this.broadcast({ t: 'table', table: this.view() });
      }, 8000);
    } else if (seat.id === this.hostId) {
      this.watchHost();
    }
    this.broadcast({ t: 'table', table: this.view() });
  }

  // A host who is gone for HOST_GRACE_MS hands the game to the first player still connected.
  watchHost() {
    clearTimeout(this.hostTimer);
    this.hostTimer = setTimeout(() => {
      const host = this.humans.find(h => h.id === this.hostId);
      if (host && host.socket) return;
      const next = this.humans.find(h => h.socket);
      if (!next) return;
      this.hostId = next.id;
      this.broadcast({ t: 'table', table: this.view() });
    }, HOST_GRACE_MS);
  }

  // ---- What players see ----------------------------------------------------------------------
  // The seats in order: people in the order they joined, then the bots. Every player builds the
  // board from this same list, so seat 1 is the same person on every screen.
  view() {
    const people = this.humans.map(h => ({ user_id: h.id, username: h.name, avatar: h.avatar, color: h.color, is_ai: false, is_host: h.id === this.hostId, connected: !!h.socket }));
    const bots = this.bots.map(b => ({ user_id: b.id, username: b.name, avatar: '🤖', color: null, is_ai: true, level: b.level, is_host: false, connected: true }));
    return { code: this.code, host_id: this.hostId, started: this.started, rules: { ...this.rules }, players: [...people, ...bots].map((p, slot) => ({ ...p, slot })) };
  }

  stateFrame() {
    return `{"t":"state","round":${this.round},"seq":${this.seq},"state":${this.lastState}}`;
  }

  send(socket, msg) {
    try { socket.send(JSON.stringify(msg)); } catch { }
  }

  broadcast(msg, except) {
    const text = JSON.stringify(msg);
    for (const h of this.humans) if (h !== except && h.socket) { try { h.socket.send(text); } catch { } }
  }

  refuse(socket, code, message) {
    this.send(socket, { t: 'error', code, message });
    try { socket.close(4001, code); } catch { }
    return null;
  }
}

// ---- The two routes the worker passes here ---------------------------------------------------
//   POST /dotsandboxes/api/table            makes a table and answers {code}
//   GET  /dotsandboxes/table/<CODE>         (WebSocket) joins it
export async function tableRoute(request, env, url) {
  if (url.pathname === '/dotsandboxes/api/table') {
    if (request.method !== 'POST') return new Response('POST', { status: 405 });
    for (let tries = 0; tries < 5; tries++) {
      const code = makeCode();
      const stub = env.DB_TABLES.get(env.DB_TABLES.idFromName(code));
      const r = await stub.fetch(`https://table/create?code=${code}`);
      if (r.ok) return new Response(JSON.stringify({ code }), { headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' } });
    }
    return new Response(JSON.stringify({ error: 'busy' }), { status: 503 });
  }
  const code = url.pathname.slice('/dotsandboxes/table/'.length).toUpperCase();
  if (!validCode(code)) return new Response('Unknown table', { status: 404 });
  return env.DB_TABLES.get(env.DB_TABLES.idFromName(code)).fetch(request);
}
