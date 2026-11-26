// table-client.js
// FastTrack with friends, the browser side (Ken, 2026-11-07). It talks to the table on
// playbutterflyfx.com (fasttrack-table.js) and gives the game the same multiplayer object it
// already knows (KGMultiplayer), so the game's own online code runs unchanged.
//
// Two pieces:
//   FTTable        the one connection to a table, shared by everything on the page
//   KGMultiplayer  what the game uses: connect, joinByCode, sendAction, sendGameState, chat, on(...)
//
// Who you are at a table is a random id kept for this browser tab (sessionStorage), so a reload or
// the move from the table page into the game puts you back in the same seat.
(function () {
  'use strict';

  const CODE_PATTERN = /^[ABCDEFGHJKMNPQRSTUVWXYZ23456789]{5}$/;

  function myClientId() {
    let id = null;
    try { id = sessionStorage.getItem('db_table_client'); } catch (_) { }
    if (!id) {
      id = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()).slice(2));
      try { sessionStorage.setItem('db_table_client', id); } catch (_) { }
    }
    return id;
  }

  function myName() {
    try { return localStorage.getItem('username') || 'Player'; } catch (_) { return 'Player'; }
  }

  function myAvatar() {
    try {
      const a = JSON.parse(localStorage.getItem('kg_avatar') || 'null');
      return (a && (a.emoji || a.id)) || '🎮';
    } catch (_) { return '🎮'; }
  }

  // The color picked on the table page (0-5), or null for "any".
  function myColor() {
    try { const c = localStorage.getItem('db_color'); return /^[0-5]$/.test(c || '') ? Number(c) : null; } catch (_) { return null; }
  }

  // ---- The connection ----------------------------------------------------------------------------
  const FTTable = {
    code: '',
    socket: null,
    you: null,
    table: null,
    listeners: [],
    retries: 0,
    stopped: false,

    // Join a table. Calling it again with the same code does nothing.
    open(code) {
      code = String(code || '').toUpperCase().trim();
      if (!CODE_PATTERN.test(code)) return false;
      if (this.code === code && this.socket) return true;
      this.close();
      this.code = code;
      this.stopped = false;
      this.connect();
      return true;
    },

    connect() {
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const socket = new WebSocket(`${proto}//${location.host}/dotsandboxes/table/${this.code}`);
      this.socket = socket;
      socket.onopen = () => {
        this.retries = 0;
        socket.send(JSON.stringify({ t: 'hello', clientId: myClientId(), name: myName(), avatar: myAvatar(), color: myColor() }));
        this.emit({ t: 'open' });
      };
      socket.onmessage = (event) => {
        let msg; try { msg = JSON.parse(event.data); } catch (_) { return; }
        if (msg.t === 'welcome') { this.you = msg.you; this.table = msg.table; }
        if (msg.t === 'table' || msg.t === 'start') this.table = msg.table;
        if (msg.t === 'error' && ['no_table', 'started', 'full'].includes(msg.code)) this.stopped = true;
        this.emit(msg);
      };
      socket.onclose = () => {
        if (this.socket !== socket) return;
        this.socket = null;
        this.emit({ t: 'closed' });
        if (this.stopped) return;
        // Dropped: try again, waiting a little longer each time (up to 10 seconds).
        const wait = Math.min(10000, 500 * Math.pow(2, this.retries++));
        setTimeout(() => { if (!this.stopped && !this.socket && this.code) this.connect(); }, wait);
      };
    },

    close() {
      this.stopped = true;
      if (this.socket) { try { this.socket.close(); } catch (_) { } }
      this.socket = null;
    },

    send(msg) {
      if (!this.socket || this.socket.readyState !== 1) return false;
      this.socket.send(JSON.stringify(msg));
      return true;
    },

    on(fn) { this.listeners.push(fn); },
    emit(msg) { for (const fn of this.listeners.slice()) { try { fn(msg); } catch (e) { console.warn('[ft-table]', e); } } },

    // The table as the game reads a session: { session_code, host_id, players: [...] }.
    session() {
      if (!this.table) return null;
      return { session_code: this.table.code, session_id: this.table.code, host_id: this.table.host_id, players: this.table.players, started: this.table.started };
    },
  };

  // The table this page belongs to: the game page says so in its address (?mode=private&code=...).
  function pageTableCode() {
    const usp = new URLSearchParams(location.search);
    const code = String(usp.get('code') || '').toUpperCase();
    return usp.get('mode') === 'private' && CODE_PATTERN.test(code) ? code : '';
  }

  // The round this page is playing: 0 for the first game at the table, N after the Nth Play again (?round=N).
  // Every board and move carries it, and anything from another round is ignored here and refused by the
  // table: a late board from the round just finished can never replace the new round's (Ken, 2026-11-20).
  function pageRound() { return Number(new URLSearchParams(location.search).get('round')) || 0; }

  // ---- What the game uses ------------------------------------------------------------------------
  class KGMultiplayer {
    constructor(gameId) {
      this.gameId = gameId;
      this.handlers = {};
      this.connected = false;
      this.isHost = false;
      this.userId = null;
      this.session = null;
      FTTable.on((msg) => this.hear(msg));
      this.sync();
    }

    on(event, fn) { (this.handlers[event] = this.handlers[event] || []).push(fn); return this; }
    emit(event, data) { for (const fn of (this.handlers[event] || []).slice()) { try { fn(data); } catch (e) { console.warn('[ft-table]', event, e); } } }

    // Copies who I am and who is at the table from the shared connection.
    sync() {
      this.userId = FTTable.you;
      this.session = FTTable.session();
      this.isHost = !!(this.session && this.userId && this.session.host_id === this.userId);
      this.connected = !!(FTTable.socket && FTTable.socket.readyState === 1);
    }

    hear(msg) {
      this.sync();
      if (msg.t === 'open') this.emit('connected', {});
      else if (msg.t === 'closed') this.emit('disconnected', {});
      else if (msg.t === 'welcome') { this.emit('authenticated', { userId: msg.you }); this.emit('session_update', this.session); }
      else if (msg.t === 'table' || msg.t === 'start') this.emit('session_update', this.session);
      else if (msg.t === 'action') { if ((msg.round | 0) === pageRound() && !this.leaving) this.emit('game_action', { action: msg.action, payload: msg.payload, from: msg.from }); }
      else if (msg.t === 'state') { if ((msg.round | 0) === pageRound() && !this.leaving) this.emit('game_state', { state: msg.state, seq: msg.seq }); }
      else if (msg.t === 'chat') this.emit('chat', msg);
      else if (msg.t === 'replay') {
        // Play again: every screen reloads into the next round, same seats, a new deal. From this moment the
        // old round's page sends and applies nothing more.
        this.leaving = true;
        const url = new URL(location.href);
        url.searchParams.set('round', String(msg.round | 0));
        if (msg.first) url.searchParams.set('first', String(msg.first)); else url.searchParams.delete('first');   // who opens
        this.emit('replay_started', { round: msg.round | 0, url: url.toString() });
      }
      else if (msg.t === 'error') this.emit('error', msg.message);
    }

    // The game page joins its own table; with no table in the address this does nothing (solo).
    connect() {
      const code = pageTableCode();
      if (code) FTTable.open(code);
      if (FTTable.you) setTimeout(() => { this.sync(); this.emit('connected', {}); this.emit('authenticated', { userId: FTTable.you }); this.emit('session_update', this.session); }, 0);
    }

    joinByCode(code) {
      code = String(code || '').toUpperCase();
      if (code && code === pageTableCode()) FTTable.open(code);
    }

    sendAction(action, payload) { if (!this.leaving) FTTable.send({ t: 'action', round: pageRound(), action, payload: payload || {} }); }
    sendGameState(state) { if (!this.leaving) FTTable.send({ t: 'state', round: pageRound(), state: state && state.fasttrack ? state.fasttrack : state }); }
    chat(payload) { FTTable.send({ t: 'chat', payload }); }
    sendReplayRequest(first) { FTTable.send({ t: 'replay', first: first || null }); }   // host only; first = the winner's seat id
    disconnect() { /* the page is closing; the table notices by itself */ }
  }

  window.FTTable = FTTable;
  window.KGMultiplayer = KGMultiplayer;
})();
