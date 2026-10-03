'use strict';
// Pricing Engine dashboard. Streams the binary delta protocol (docs/PROTOCOL.md)
// over /ws, renders a watchlist, an instrument panel with candles from
// /api/candles, and engine health from /metrics. No dependencies.
(() => {
  const $ = (id) => document.getElementById(id);

  // ---------- constants & helpers ----------
  const CRYPTO = new Set(['BTCUSD', 'ETHUSD', 'BNBUSD', 'SOLUSD', 'XRPUSD', 'ADAUSD', 'DOGUSD', 'DOTUSD',
    'LTCUSD', 'BCHUSD', 'XLMUSD', 'TRXUSD', 'UNIUSD', 'FILUSD', 'AVXUSD']);
  const METALS = new Set(['XAUUSD', 'XAGUSD']);
  const category = (s) => (CRYPTO.has(s) ? 'crypto' : METALS.has(s) ? 'metals' : 'fx');
  const CAT_LABEL = { fx: 'FX', metals: 'Metal', crypto: 'Crypto' };
  const IDLE_MS = 5 * 60 * 1000;
  const SPARK_MS = 60 * 60 * 1000;
  const CANDLE_LIMIT = { 60000: 240, 300000: 288, 900000: 96, 3600000: 24 };

  const store = {
    get(k, d) { try { const v = localStorage.getItem('pe.' + k); return v === null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem('pe.' + k, JSON.stringify(v)); } catch { /* storage unavailable */ } },
  };
  const nowUs = () => (performance.timeOrigin + performance.now()) * 1000;

  const fmtPx = (v, d) => (Number.isFinite(v) ? v.toFixed(d) : '—');
  const fmtPct = (v) => (Number.isFinite(v) ? (v > 0 ? '+' : '') + v.toFixed(2) + '%' : '—');
  const fmtInt = (v) => (Number.isFinite(v) ? Math.round(v).toLocaleString() : '—');
  function fmtCompact(v) {
    if (!Number.isFinite(v)) return '—';
    if (Number.isInteger(v) && Math.abs(v) < 1e4) return String(v);
    const a = Math.abs(v);
    if (a >= 1e6) return (v / 1e6).toFixed(2) + 'M';
    if (a >= 1e4) return (v / 1e3).toFixed(1) + 'k';
    if (a >= 100) return Math.round(v).toLocaleString();
    return v.toFixed(a >= 10 ? 1 : 2);
  }
  function fmtBytes(v) {
    if (!Number.isFinite(v)) return '—';
    if (v >= 1048576) return (v / 1048576).toFixed(2) + ' MB';
    if (v >= 1024) return (v / 1024).toFixed(1) + ' KB';
    return Math.round(v) + ' B';
  }
  function fmtDur(sec) {
    if (!Number.isFinite(sec)) return '—';
    if (sec < 1e-6) return (sec * 1e9).toFixed(0) + ' ns';
    if (sec < 1e-3) return (sec * 1e6).toFixed(sec < 1e-5 ? 2 : 0) + ' µs';
    if (sec < 1) return (sec * 1e3).toFixed(sec < 0.01 ? 2 : 1) + ' ms';
    return sec.toFixed(2) + ' s';
  }
  function fmtAgo(ms) {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 5) return 'just now';
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    if (s < 86400) return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm ago';
    return Math.floor(s / 86400) + 'd ago';
  }
  const pad2 = (n) => String(n).padStart(2, '0');

  // Fractional digits actually used by v (at most max). Upstream pads some
  // instruments (BTCUSD arrives as 84727.43000), so display precision is the
  // largest precision observed rather than the padded wire precision.
  function sigDecimals(v, max) {
    for (let k = 0; k < max; k++) {
      const x = v * 10 ** k;
      if (Math.abs(x - Math.round(x)) <= 1e-9 * Math.max(1, Math.abs(x))) return k;
    }
    return max;
  }

  // ---------- state ----------
  const S = {
    names: new Map(),   // wire id -> symbol
    quotes: new Map(),  // symbol -> quote state
    order: [],          // visible symbols in display order
    basis: 'mid',
    upstream: false,
    connected: false,
    selected: store.get('sel', 'BTCUSD'),
    tab: store.get('tab', 'all'),
    filter: '',
    sort: store.get('sort', { key: 'symbol', dir: 1 }),
    favs: new Set(store.get('favs', [])),
    interval: store.get('interval', 60000),
    candles: [],
    candleSym: null,
    dirty: new Set(),
    orderDirty: true,
    net: { frames: 0, bytes: 0, updates: 0, lat: [] },
    startedAt: Date.now(),
  };
  if (!CANDLE_LIMIT[S.interval]) S.interval = 60000;

  const basisPx = (q) => (S.basis === 'bid' ? q.bid : S.basis === 'ask' ? q.ask : (q.bid + q.ask) / 2);

  // ---------- binary protocol decoder ----------
  function makeDecoder() {
    const st = new Map(); // id -> {d, f:[bid, ask, high, low, chg, time]}
    return function decode(buf) {
      const u8 = new Uint8Array(buf);
      const dv = new DataView(buf);
      if (u8[0] !== 1) throw new Error('unknown frame type');
      const ingestUs = Number(dv.getBigUint64(1, true));
      let p = 9;
      const uvar = () => {
        let x = 0, mul = 1, b;
        do { b = u8[p++]; x += (b & 0x7f) * mul; mul *= 128; } while (b & 0x80);
        return x;
      };
      const svar = () => { const u = uvar(); return u % 2 ? -(u + 1) / 2 : u / 2; };
      const seq = uvar();
      const out = [];
      while (p < u8.length) {
        const id = uvar();
        const mask = u8[p++];
        let s = st.get(id);
        if (mask & 1) {
          s = { d: u8[p++], f: new Array(6) };
          for (let i = 0; i < 6; i++) s.f[i] = svar();
          st.set(id, s);
        } else {
          if (!s) throw new Error('delta before full record');
          for (let i = 0; i < 6; i++) if (mask & (2 << i)) s.f[i] += svar();
        }
        const k = 10 ** s.d;
        out.push({ id, d: s.d, bid: s.f[0] / k, ask: s.f[1] / k, high: s.f[2] / k, low: s.f[3] / k, chg: s.f[4] / 1e4, t: s.f[5] });
      }
      return { seq, ingestUs, quotes: out };
    };
  }

  // ---------- connection ----------
  let backoff = 500;
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws?format=binary&symbols=*`);
    ws.binaryType = 'arraybuffer';
    const decode = makeDecoder(); // delta state is per connection
    ws.onopen = () => { S.connected = true; backoff = 500; setStatus(); };
    ws.onclose = () => {
      S.connected = false;
      setStatus();
      setTimeout(connect, backoff);
      backoff = Math.min(backoff * 2, 8000);
    };
    ws.onmessage = (ev) => {
      if (typeof ev.data === 'string') return onControl(JSON.parse(ev.data));
      const f = decode(ev.data);
      S.net.frames++;
      S.net.bytes += ev.data.byteLength;
      S.net.updates += f.quotes.length;
      if (f.seq > 1) S.net.lat.push((nowUs() - f.ingestUs) / 1000);
      for (const q of f.quotes) onQuote(q);
      schedule();
    };
  }

  function onControl(m) {
    switch (m.t) {
      case 'welcome':
        S.basis = m.basis || 'mid';
        S.upstream = !!m.upstream;
        $('m-basis').textContent = S.basis;
        setStatus();
        break;
      case 'symbols':
        for (const e of m.symbols) S.names.set(e.i, e.s);
        S.orderDirty = true;
        schedule();
        break;
      case 'status':
        S.upstream = !!m.upstream;
        setStatus();
        break;
    }
  }

  function setStatus() {
    const sock = $('pill-socket'), feed = $('pill-feed');
    sock.className = 'pill ' + (S.connected ? 'ok' : 'bad');
    sock.lastElementChild.textContent = S.connected ? 'connected' : 'reconnecting';
    feed.className = 'pill ' + (!S.connected ? '' : S.upstream ? 'ok' : 'bad');
    feed.lastElementChild.textContent = !S.connected ? '—' : S.upstream ? 'live' : 'down';
    $('banner').hidden = !(S.connected && !S.upstream);
  }

  function onQuote(u) {
    const sym = S.names.get(u.id);
    if (!sym) return;
    let q = S.quotes.get(sym);
    if (!q) {
      q = { sym, cat: category(sym), spark: [], dirBid: 0, dirAsk: 0 };
      S.quotes.set(sym, q);
      S.orderDirty = true;
      loadSpark(sym);
    }
    q.dirBid = q.bid === undefined ? 0 : Math.sign(u.bid - q.bid);
    q.dirAsk = q.ask === undefined ? 0 : Math.sign(u.ask - q.ask);
    Object.assign(q, { d: u.d, bid: u.bid, ask: u.ask, high: u.high, low: u.low, chg: u.chg, t: u.t });
    q.dd = Math.max(q.dd || Math.min(u.d, 2), sigDecimals(u.bid, u.d), sigDecimals(u.ask, u.d));
    const px = basisPx(q);
    pushPoint(q.spark, Math.floor(q.t / 60000) * 60000, px);
    if (sym === S.candleSym) liveCandle(q, px);
    S.dirty.add(sym);
  }

  function pushPoint(arr, t, c) {
    const last = arr[arr.length - 1];
    if (last && last.t === t) last.c = c;
    else if (!last || t > last.t) {
      arr.push({ t, c });
      while (arr.length && arr[0].t < t - SPARK_MS) arr.shift();
    }
  }

  // ---------- rendering loop ----------
  let rafPending = false;
  function schedule() {
    if (!rafPending) { rafPending = true; requestAnimationFrame(render); }
  }
  function render() {
    rafPending = false;
    if (S.orderDirty) { S.orderDirty = false; rebuildList(); }
    for (const sym of S.dirty) updateRow(S.quotes.get(sym), true);
    if (S.dirty.has(S.selected)) { updateDetail(true); drawChart(); }
    S.dirty.clear();
  }

  // ---------- watchlist ----------
  const tbody = $('rows');

  function makeRow(q) {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td class="c-star"><button class="star" aria-label="Add to watchlist">★</button></td>' +
      '<td class="c-sym"><div class="sym"></div><div class="sub"><span class="cat"></span><span class="badge-idle" hidden>idle</span></div></td>' +
      '<td class="num c-bid"></td><td class="num c-ask"></td><td class="num c-spr hide-sm"></td>' +
      '<td class="num c-chg"><span class="chg"></span></td>' +
      '<td class="c-rng hide-md"><div class="range"><div class="fill"></div><i></i></div></td>' +
      '<td class="c-spark hide-md"><svg viewBox="0 0 100 28" preserveAspectRatio="none"><polyline/></svg></td>';
    const el = {
      tr,
      star: tr.querySelector('.star'),
      idle: tr.querySelector('.badge-idle'),
      bid: tr.querySelector('.c-bid'),
      ask: tr.querySelector('.c-ask'),
      spr: tr.querySelector('.c-spr'),
      chg: tr.querySelector('.chg'),
      rng: tr.querySelector('.c-rng'),
      fill: tr.querySelector('.range .fill'),
      mark: tr.querySelector('.range i'),
      spark: tr.querySelector('.c-spark'),
      poly: tr.querySelector('polyline'),
    };
    tr.querySelector('.sym').textContent = q.sym;
    tr.querySelector('.cat').textContent = CAT_LABEL[q.cat];
    tr.addEventListener('click', () => select(q.sym));
    el.star.addEventListener('click', (e) => { e.stopPropagation(); toggleFav(q.sym); });
    q.el = el;
    return el;
  }

  function flash(el, dir) {
    if (!dir) return;
    el.classList.remove('flash-up', 'flash-down');
    void el.offsetWidth; // restart the animation
    el.classList.add(dir > 0 ? 'flash-up' : 'flash-down');
  }

  function rangePos(q) {
    const px = basisPx(q);
    return q.high > q.low ? Math.min(1, Math.max(0, (px - q.low) / (q.high - q.low))) : 0.5;
  }
  const spreadBps = (q) => ((q.ask - q.bid) / ((q.ask + q.bid) / 2)) * 1e4;

  function updateRow(q, animate) {
    if (!q || q.bid === undefined) return;
    const el = q.el || makeRow(q);
    el.bid.textContent = fmtPx(q.bid, q.dd);
    el.ask.textContent = fmtPx(q.ask, q.dd);
    if (animate) { flash(el.bid, q.dirBid); flash(el.ask, q.dirAsk); }
    el.spr.textContent = spreadBps(q).toFixed(1) + ' bp';
    el.chg.textContent = fmtPct(q.chg);
    el.chg.className = 'chg' + (q.chg > 0.00005 ? ' pos' : q.chg < -0.00005 ? ' neg' : '');
    const pos = rangePos(q);
    el.fill.style.width = (pos * 100).toFixed(1) + '%';
    el.mark.style.left = (pos * 100).toFixed(1) + '%';
    el.rng.title = `24h low ${fmtPx(q.low, q.dd)} · high ${fmtPx(q.high, q.dd)}`;
    drawSpark(q);
    const idle = Date.now() - q.t > IDLE_MS;
    el.tr.classList.toggle('idle', idle);
    el.idle.hidden = !idle;
    el.tr.title = idle ? `No price change since ${new Date(q.t).toLocaleString()}` : '';
    el.star.classList.toggle('on', S.favs.has(q.sym));
    el.tr.classList.toggle('selected', q.sym === S.selected);
  }

  function drawSpark(q) {
    const pts = q.spark;
    const el = q.el;
    if (pts.length < 2) { el.poly.setAttribute('points', ''); el.spark.className = 'c-spark hide-md'; return; }
    let lo = Infinity, hi = -Infinity;
    for (const p of pts) { lo = Math.min(lo, p.c); hi = Math.max(hi, p.c); }
    const t0 = pts[0].t, span = Math.max(pts[pts.length - 1].t - t0, 1);
    const r = hi - lo || 1;
    el.poly.setAttribute('points', pts.map((p) =>
      `${(((p.t - t0) / span) * 100).toFixed(2)},${(26 - ((p.c - lo) / r) * 24).toFixed(2)}`).join(' '));
    const trend = pts[pts.length - 1].c - pts[0].c;
    el.spark.className = 'c-spark hide-md' + (trend > 0 ? ' pos' : trend < 0 ? ' neg' : '');
  }

  function matches(q) {
    if (S.tab === 'fav' && !S.favs.has(q.sym)) return false;
    if (S.tab !== 'all' && S.tab !== 'fav' && q.cat !== S.tab) return false;
    return !S.filter || q.sym.includes(S.filter);
  }

  const SORTERS = {
    symbol: (a, b) => a.sym.localeCompare(b.sym),
    bid: (a, b) => a.bid - b.bid,
    ask: (a, b) => a.ask - b.ask,
    spread: (a, b) => spreadBps(a) - spreadBps(b),
    chg: (a, b) => a.chg - b.chg,
  };

  function rebuildList() {
    const all = [...S.quotes.values()].filter((q) => q.bid !== undefined);
    const visible = all.filter(matches);
    const cmp = SORTERS[S.sort.key] || SORTERS.symbol;
    visible.sort((a, b) => cmp(a, b) * S.sort.dir || a.sym.localeCompare(b.sym));
    S.order = visible.map((q) => q.sym);

    const frag = document.createDocumentFragment();
    for (const q of visible) {
      const el = q.el || makeRow(q);
      if (!q.el.rendered) { updateRow(q, false); q.el.rendered = true; }
      frag.appendChild(el.tr);
    }
    tbody.replaceChildren(frag);

    const counts = { all: all.length, fx: 0, metals: 0, crypto: 0, fav: 0 };
    for (const q of all) { counts[q.cat]++; if (S.favs.has(q.sym)) counts.fav++; }
    for (const b of document.querySelectorAll('#tabs button')) {
      b.setAttribute('aria-selected', String(b.dataset.tab === S.tab));
      b.querySelector('.count').textContent = counts[b.dataset.tab] || '';
    }
    for (const th of document.querySelectorAll('.wl th[data-sort]')) {
      th.setAttribute('aria-sort', th.dataset.sort === S.sort.key ? (S.sort.dir > 0 ? 'ascending' : 'descending') : 'none');
    }

    const empty = $('wl-empty');
    empty.hidden = visible.length > 0;
    empty.textContent = all.length === 0 ? 'Waiting for prices…'
      : S.tab === 'fav' && !S.filter ? 'No instruments on your watchlist yet. Click ★ on any row to add one.'
        : 'No symbols match.';
    const active = all.filter((q) => Date.now() - q.t <= IDLE_MS).length;
    $('wl-foot').textContent = `${all.length} of ${S.names.size || '—'} symbols streaming · ${active} active in the last 5 min`;

    if (!S.quotes.has(S.selected) && S.order.length && Date.now() - S.startedAt > 1500) select(S.order[0]);
  }

  function toggleFav(sym) {
    if (S.favs.has(sym)) S.favs.delete(sym); else S.favs.add(sym);
    store.set('favs', [...S.favs]);
    S.orderDirty = true;
    const q = S.quotes.get(sym);
    if (q && q.el) q.el.star.classList.toggle('on', S.favs.has(sym));
    schedule();
  }

  function select(sym) {
    if (S.selected === sym && S.candleSym === sym) return;
    const prev = S.quotes.get(S.selected);
    if (prev && prev.el) prev.el.tr.classList.remove('selected');
    S.selected = sym;
    store.set('sel', sym);
    const q = S.quotes.get(sym);
    if (q && q.el) {
      q.el.tr.classList.add('selected');
      q.el.tr.scrollIntoView({ block: 'nearest' });
    }
    S.candles = [];
    S.candleSym = null;
    chart.hover = null;
    updateDetail(false);
    drawChart();
    loadCandles();
  }

  // ---------- instrument panel ----------
  function bigPrice(el, v, d) {
    const s = fmtPx(v, d);
    if (d < 3 || s === '—') { el.textContent = s; return; }
    // Emphasise the "pips": the two digits before the last one.
    el.innerHTML = '';
    const lead = document.createElement('span');
    const pip = document.createElement('span');
    const frac = document.createElement('span');
    lead.textContent = s.slice(0, -3);
    pip.textContent = s.slice(-3, -1);
    frac.textContent = s.slice(-1);
    pip.className = 'pip';
    frac.className = 'frac';
    el.append(lead, pip, frac);
  }

  function updateDetail(animate) {
    const q = S.quotes.get(S.selected);
    $('d-sym').textContent = S.selected;
    $('d-cat').textContent = CAT_LABEL[category(S.selected)];
    document.title = q && q.bid !== undefined ? `${S.selected} ${fmtPx(q.bid, q.dd)} · Pricing Engine` : 'Pricing Engine';
    if (!q || q.bid === undefined) {
      for (const id of ['d-bid', 'd-ask', 'd-spread', 'd-chg', 'd-low', 'd-high']) $(id).textContent = '—';
      $('d-spread-bps').textContent = '';
      $('d-age').textContent = 'No data received for this symbol yet';
      return;
    }
    bigPrice($('d-bid'), q.bid, q.dd);
    bigPrice($('d-ask'), q.ask, q.dd);
    if (animate) { flash($('d-sellbox'), q.dirBid); flash($('d-buybox'), q.dirAsk); }
    $('d-spread').textContent = fmtPx(q.ask - q.bid, q.dd);
    $('d-spread-bps').textContent = spreadBps(q).toFixed(1) + ' bp';
    const chg = $('d-chg');
    chg.textContent = fmtPct(q.chg);
    chg.className = q.chg > 0.00005 ? 'up' : q.chg < -0.00005 ? 'down' : '';
    $('d-low').textContent = fmtPx(q.low, q.dd);
    $('d-high').textContent = fmtPx(q.high, q.dd);
    const pos = rangePos(q);
    $('d-range-fill').style.width = (pos * 100).toFixed(1) + '%';
    $('d-range-mark').style.left = (pos * 100).toFixed(1) + '%';
    $('d-rl-lo').textContent = fmtPx(q.low, q.dd);
    $('d-rl-hi').textContent = fmtPx(q.high, q.dd);
    updateAge();
  }

  function updateAge() {
    const q = S.quotes.get(S.selected);
    if (!q || q.t === undefined) return;
    const age = Date.now() - q.t;
    $('d-age').innerHTML = '';
    const line1 = document.createElement('div');
    line1.textContent = (age > IDLE_MS ? 'Idle · last change ' : 'Last change ') + fmtAgo(age);
    const line2 = document.createElement('div');
    line2.textContent = new Date(q.t).toLocaleString();
    $('d-age').append(line1, line2);
  }

  // ---------- candles ----------
  async function loadCandles() {
    const sym = S.selected, iv = S.interval;
    let data = [];
    try {
      const r = await fetch(`/api/candles?symbol=${encodeURIComponent(sym)}&interval=${iv / 60000}m&limit=${CANDLE_LIMIT[iv]}`);
      if (r.ok) data = await r.json();
    } catch { /* keep empty */ }
    if (sym !== S.selected || iv !== S.interval) return;
    S.candles = data.map((c) => ({ t: c.t, o: c.o, h: c.h, l: c.l, c: c.c }));
    S.candleSym = sym;
    drawChart();
  }

  function liveCandle(q, px) {
    const iv = S.interval;
    const b = Math.floor(q.t / iv) * iv;
    const last = S.candles[S.candles.length - 1];
    if (last && last.t === b) {
      last.h = Math.max(last.h, px);
      last.l = Math.min(last.l, px);
      last.c = px;
    } else if (!last || b > last.t) {
      S.candles.push({ t: b, o: px, h: px, l: px, c: px });
      if (S.candles.length > CANDLE_LIMIT[iv]) S.candles.shift();
    }
  }

  async function loadSpark(sym) {
    try {
      const r = await fetch(`/api/candles?symbol=${encodeURIComponent(sym)}&interval=1m&limit=60`);
      if (!r.ok) return;
      const data = await r.json();
      const q = S.quotes.get(sym);
      if (!q) return;
      const live = q.spark;
      const merged = data.map((c) => ({ t: c.t, c: c.c }));
      const lastT = merged.length ? merged[merged.length - 1].t : -Infinity;
      for (const p of live) {
        if (p.t > lastT) merged.push(p);
        else if (p.t === lastT) merged[merged.length - 1].c = p.c;
      }
      q.spark = merged;
      S.dirty.add(sym);
      schedule();
    } catch { /* sparkline stays live-only */ }
  }

  // ---------- chart ----------
  const chart = { cv: $('chart'), hover: null, geom: null };

  function niceStep(range, count) {
    const rough = range / count;
    const mag = 10 ** Math.floor(Math.log10(rough));
    const n = rough / mag;
    return (n < 1.5 ? 1 : n < 3 ? 2 : n < 7 ? 5 : 10) * mag;
  }

  function drawChart() {
    const cv = chart.cv;
    const wrap = cv.parentElement;
    const W = wrap.clientWidth, H = wrap.clientHeight;
    if (!W || !H) return;
    const dpr = window.devicePixelRatio || 1;
    if (cv.width !== Math.round(W * dpr) || cv.height !== Math.round(H * dpr)) {
      cv.width = Math.round(W * dpr);
      cv.height = Math.round(H * dpr);
    }
    const ctx = cv.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);

    const data = S.candleSym === S.selected ? S.candles : [];
    $('chart-empty').hidden = data.length > 0;
    if (!data.length) { chart.geom = null; renderLegend(null); return; }

    const css = getComputedStyle(document.documentElement);
    const col = (n) => css.getPropertyValue(n).trim();
    const C = { up: col('--up'), down: col('--down'), line: col('--line-2'), muted: col('--muted'), fg: col('--fg'), panel: col('--panel'), accent: col('--accent') };
    const q = S.quotes.get(S.selected);
    const d = q ? q.dd : 5;
    ctx.font = '11px ' + col('--mono');

    // Geometry: newest candle on the right, as many as fit.
    let lo = Infinity, hi = -Infinity;
    for (const c of data) { lo = Math.min(lo, c.l); hi = Math.max(hi, c.h); }
    const axisW = Math.ceil(ctx.measureText(fmtPx(hi, d)).width) + 18;
    const padT = 12, padB = 24;
    const plotW = W - axisW, plotH = H - padT - padB;
    const step = Math.max(4, Math.min(16, plotW / Math.max(data.length, 30)));
    const n = Math.min(data.length, Math.floor(plotW / step));
    const vis = data.slice(-n);
    lo = Infinity; hi = -Infinity;
    for (const c of vis) { lo = Math.min(lo, c.l); hi = Math.max(hi, c.h); }
    if (hi === lo) { const e = Math.abs(hi) * 1e-4 || 1e-4; hi += e; lo -= e; }
    const pad = (hi - lo) * 0.1;
    hi += pad; lo -= pad;
    const y = (v) => padT + ((hi - v) / (hi - lo)) * plotH;
    const x = (i) => plotW - (n - i - 0.5) * step;
    chart.geom = { n, step, plotW, plotH, padT, hi, lo, vis, axisW };

    // Horizontal grid + price axis.
    const ys = niceStep(hi - lo, 5);
    const yd = Math.min(d, Math.max(0, -Math.floor(Math.log10(ys)) + 1));
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    for (let v = Math.ceil(lo / ys) * ys; v <= hi; v += ys) {
      const py = Math.round(y(v)) + 0.5;
      ctx.strokeStyle = C.line;
      ctx.beginPath(); ctx.moveTo(0, py); ctx.lineTo(plotW, py); ctx.stroke();
      ctx.fillStyle = C.muted;
      ctx.fillText(v.toFixed(yd), plotW + 8, py);
    }

    // Time axis.
    const iv = S.interval;
    const every = Math.max(1, Math.ceil(80 / step));
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    for (let i = n - 1; i >= 0; i -= every) {
      const dt = new Date(vis[i].t);
      const label = iv >= 3600000 && dt.getHours() === 0 ? `${dt.getMonth() + 1}/${dt.getDate()}` : `${pad2(dt.getHours())}:${pad2(dt.getMinutes())}`;
      const px = Math.round(x(i)) + 0.5;
      ctx.strokeStyle = C.line;
      ctx.beginPath(); ctx.moveTo(px, padT); ctx.lineTo(px, padT + plotH); ctx.stroke();
      if (px - ctx.measureText(label).width / 2 < 2) continue; // would be clipped at the left edge
      ctx.fillStyle = C.muted;
      ctx.fillText(label, px, padT + plotH + 7);
    }

    // Candles.
    const bw = Math.max(1, Math.floor(step * 0.7) - (Math.floor(step * 0.7) % 2 === 0 ? 1 : 0));
    for (let i = 0; i < n; i++) {
      const c = vis[i];
      const cx = Math.round(x(i));
      ctx.fillStyle = c.c >= c.o ? C.up : C.down;
      const yh = y(c.h), yl = y(c.l);
      ctx.fillRect(cx, Math.round(yh), 1, Math.max(1, Math.round(yl - yh)));
      const top = y(Math.max(c.o, c.c)), bot = y(Math.min(c.o, c.c));
      ctx.fillRect(cx - (bw - 1) / 2, Math.round(top), bw, Math.max(1, Math.round(bot - top)));
    }

    // Last price line and tag.
    const last = vis[n - 1];
    const lc = last.c >= last.o ? C.up : C.down;
    const ly = Math.round(y(last.c)) + 0.5;
    ctx.strokeStyle = lc;
    ctx.setLineDash([3, 3]);
    ctx.beginPath(); ctx.moveTo(0, ly); ctx.lineTo(plotW, ly); ctx.stroke();
    ctx.setLineDash([]);
    axisTag(ctx, fmtPx(last.c, d), plotW, ly, axisW, lc, '#fff');

    // Crosshair.
    const h = chart.hover;
    if (h && h.i >= 0 && h.i < n) {
      const hx = Math.round(x(h.i)) + 0.5;
      const hy = Math.round(Math.min(Math.max(h.y, padT), padT + plotH)) + 0.5;
      ctx.strokeStyle = C.muted;
      ctx.setLineDash([2, 3]);
      ctx.beginPath(); ctx.moveTo(hx, padT); ctx.lineTo(hx, padT + plotH); ctx.moveTo(0, hy); ctx.lineTo(plotW, hy); ctx.stroke();
      ctx.setLineDash([]);
      const v = hi - ((hy - padT) / plotH) * (hi - lo);
      axisTag(ctx, fmtPx(v, d), plotW, hy, axisW, C.fg, C.panel);
      renderLegend(vis[h.i], d);
    } else {
      renderLegend(last, d);
    }
  }

  function axisTag(ctx, text, x0, y0, w, bg, fg) {
    ctx.fillStyle = bg;
    ctx.fillRect(x0 + 1, y0 - 9, w - 1, 18);
    ctx.fillStyle = fg;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(text, x0 + 8, y0);
  }

  function renderLegend(c, d) {
    const el = $('legend');
    el.innerHTML = '';
    if (!c) return;
    const dt = new Date(c.t);
    const parts = [
      ['', `${dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} ${pad2(dt.getHours())}:${pad2(dt.getMinutes())}`],
      ['O', fmtPx(c.o, d)], ['H', fmtPx(c.h, d)], ['L', fmtPx(c.l, d)], ['C', fmtPx(c.c, d)],
    ];
    for (const [k, v] of parts) {
      const s = document.createElement('span');
      if (k) { s.append(k + ' '); const b = document.createElement('b'); b.textContent = v; s.append(b); } else s.textContent = v;
      el.append(s);
    }
    const chg = c.o ? ((c.c - c.o) / c.o) * 100 : 0;
    const s = document.createElement('span');
    s.textContent = fmtPct(chg);
    s.className = chg > 0 ? 'up' : chg < 0 ? 'down' : '';
    el.append(s);
  }

  chart.cv.addEventListener('mousemove', (e) => {
    const g = chart.geom;
    if (!g) return;
    const r = chart.cv.getBoundingClientRect();
    const mx = e.clientX - r.left, my = e.clientY - r.top;
    if (mx > g.plotW) { chart.hover = null; drawChart(); return; }
    chart.hover = { i: g.n - 1 - Math.floor((g.plotW - mx) / g.step), y: my };
    drawChart();
  });
  chart.cv.addEventListener('mouseleave', () => { chart.hover = null; drawChart(); });
  new ResizeObserver(() => drawChart()).observe(chart.cv.parentElement);

  // ---------- engine health (/metrics) ----------
  const M = { prev: null, prevT: 0, hist: { in: [], ticks: [], frames: [], lat: [] } };

  function parseProm(text) {
    const m = new Map();
    for (const line of text.split('\n')) {
      if (!line || line[0] === '#') continue;
      const sp = line.lastIndexOf(' ');
      m.set(line.slice(0, sp), parseFloat(line.slice(sp + 1)));
    }
    return m;
  }
  const sumPrefix = (m, prefix) => { let s = 0; for (const [k, v] of m) if (k.startsWith(prefix)) s += v; return s; };
  const delta = (m, p, k) => (m.get(k) ?? 0) - (p.get(k) ?? 0);
  const deltaPrefix = (m, p, prefix) => sumPrefix(m, prefix) - sumPrefix(p, prefix);

  function histQuantile(m, p, name, qs) {
    const buckets = [];
    for (const [k, v] of m) {
      if (!k.startsWith(name + '_bucket{')) continue;
      const le = /le="([^"]+)"/.exec(k)[1];
      buckets.push({ le: le === '+Inf' ? Infinity : parseFloat(le), v: v - (p.get(k) ?? 0) });
    }
    buckets.sort((a, b) => a.le - b.le);
    const total = buckets.length ? buckets[buckets.length - 1].v : 0;
    if (!total) return null;
    const res = {};
    for (const q of qs) {
      const target = q * total;
      let prevLe = 0, prevV = 0;
      for (const b of buckets) {
        if (b.v >= target) {
          res[q] = b.le === Infinity ? prevLe : prevLe + ((b.le - prevLe) * (target - prevV)) / Math.max(b.v - prevV, 1e-9);
          break;
        }
        prevLe = b.le; prevV = b.v;
      }
    }
    let within = 0;
    for (const b of buckets) if (b.le <= 0.05) within = b.v;
    res.within50 = within / total;
    return res;
  }

  function trend(id, arr, v) {
    arr.push(Number.isFinite(v) ? v : 0);
    if (arr.length > 60) arr.shift();
    const svg = $(id);
    const max = Math.max(...arr, 1e-12);
    const pts = arr.map((a, i) => [arr.length === 1 ? 100 : (i / (arr.length - 1)) * 100, 32 - (a / max) * 28]);
    const line = pts.map((pt, i) => (i ? 'L' : 'M') + pt[0].toFixed(2) + ' ' + pt[1].toFixed(2)).join(' ');
    svg.querySelector('.line').setAttribute('d', line);
    svg.querySelector('.area').setAttribute('d', `${line} L100 34 L0 34 Z`);
  }

  function setHero(id, value, unit) {
    const el = $(id);
    el.textContent = value;
    if (unit) { const s = document.createElement('small'); s.textContent = unit; el.append(s); }
  }

  async function pollMetrics() {
    let m;
    try {
      const r = await fetch('/metrics', { cache: 'no-store' });
      m = parseProm(await r.text());
    } catch { return; }
    const t = performance.now();
    const p = M.prev;
    M.prev = m;
    const dt = (t - M.prevT) / 1000;
    M.prevT = t;
    if (!p) return;

    // Upstream.
    const up = m.get('pricing_upstream_connected') === 1;
    $('m-feed-state').innerHTML = `<span class="dot ${up ? 'ok' : 'bad'}"></span>${up ? 'connected' : 'disconnected'}`;
    const msgs = delta(m, p, 'pricing_upstream_message_bytes_count');
    const inBytes = delta(m, p, 'pricing_upstream_bytes_total');
    setHero('m-in-rate', fmtCompact(msgs / dt), 'msg/s');
    trend('t-in', M.hist.in, msgs / dt);
    $('m-in-kb').textContent = fmtBytes(inBytes / dt) + '/s';
    $('m-in-msg').textContent = msgs ? fmtBytes(inBytes / msgs) : '—';
    const dec = delta(m, p, 'pricing_upstream_decode_seconds_count');
    $('m-decode').textContent = dec ? fmtDur(delta(m, p, 'pricing_upstream_decode_seconds_sum') / dec) : '—';
    $('m-reconn').textContent = fmtInt(m.get('pricing_upstream_reconnects_total'));

    // Engine.
    const ticks = deltaPrefix(m, p, 'pricing_ticks_total{');
    const dup = delta(m, p, 'pricing_ticks_total{result="duplicate"}');
    setHero('m-ticks', fmtCompact(ticks / dt), '/s');
    trend('t-ticks', M.hist.ticks, ticks / dt);
    const tc = delta(m, p, 'pricing_engine_tick_seconds_count');
    $('m-tick-ns').textContent = tc ? fmtDur(delta(m, p, 'pricing_engine_tick_seconds_sum') / tc) : '—';
    $('m-dup').textContent = ticks ? ((dup / ticks) * 100).toFixed(1) + '%' : '—';
    const tot = (r) => m.get(`pricing_ticks_total{result="${r}"}`) ?? 0;
    $('m-rej').textContent = `${fmtCompact(tot('late'))} / ${fmtCompact(tot('spike'))} / ${fmtCompact(tot('invalid') + tot('future') + tot('too_old') + tot('bad_symbol'))}`;
    const fc = delta(m, p, 'pricing_persist_flush_seconds_count');
    $('m-flush').textContent = fc ? fmtDur(delta(m, p, 'pricing_persist_flush_seconds_sum') / fc) : 'idle';

    // Distribution.
    const frames = deltaPrefix(m, p, 'pricing_ws_frames_sent_total{');
    const outBytes = deltaPrefix(m, p, 'pricing_ws_bytes_sent_total{');
    const clients = m.get('pricing_ws_clients') ?? 0;
    setHero('m-frames', fmtCompact(frames / dt), '/s');
    trend('t-frames', M.hist.frames, frames / dt);
    $('m-clients').textContent = fmtInt(clients);
    $('m-out-kb').textContent = fmtBytes(outBytes / dt) + '/s';
    $('m-frame').textContent = frames ? fmtBytes(outBytes / frames) : '—';
    $('m-ratio').textContent = inBytes && clients ? ((outBytes / clients / inBytes) * 100).toFixed(1) + '%' : '—';

    // Latency.
    const lq = histQuantile(m, p, 'pricing_e2e_latency_seconds', [0.5, 0.9, 0.99]);
    if (lq) {
      setHero('m-p50', fmtDur(lq[0.5]));
      $('m-p99').textContent = `${fmtDur(lq[0.9])} / ${fmtDur(lq[0.99])}`;
      $('m-budget').textContent = (lq.within50 * 100).toFixed(2) + '%';
      trend('t-lat', M.hist.lat, lq[0.5] * 1000);
    } else {
      trend('t-lat', M.hist.lat, 0);
    }
    $('m-updated').textContent = 'from /metrics · ' + new Date().toLocaleTimeString();
  }

  // ---------- header stats (this browser) ----------
  function headerStats() {
    const n = S.net;
    const lat = n.lat.sort((a, b) => a - b);
    const p50 = lat.length ? lat[Math.floor(lat.length / 2)] : null;
    if (p50 !== null) {
      $('h-lat').textContent = p50 < 10 ? p50.toFixed(2) + ' ms' : p50.toFixed(0) + ' ms';
      $('m-client-lat').textContent = p50.toFixed(2) + ' ms';
    }
    $('h-ups').textContent = fmtCompact(n.updates);
    $('h-bw').textContent = fmtBytes(n.bytes) + '/s';
    $('h-bpu').textContent = n.updates ? (n.bytes / n.updates).toFixed(1) + ' B' : '—';
    S.net = { frames: 0, bytes: 0, updates: 0, lat: [] };
  }

  // ---------- controls ----------
  document.getElementById('tabs').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-tab]');
    if (!b) return;
    S.tab = b.dataset.tab;
    store.set('tab', S.tab);
    S.orderDirty = true;
    schedule();
  });

  const search = $('search');
  search.addEventListener('input', () => {
    S.filter = search.value.trim().toUpperCase();
    S.orderDirty = true;
    schedule();
  });

  for (const th of document.querySelectorAll('.wl th[data-sort]')) {
    th.addEventListener('click', () => {
      const key = th.dataset.sort;
      S.sort = S.sort.key === key ? { key, dir: -S.sort.dir } : { key, dir: key === 'symbol' ? 1 : -1 };
      store.set('sort', S.sort);
      S.orderDirty = true;
      schedule();
    });
  }

  const ivButtons = document.querySelectorAll('#intervals button');
  function syncIntervals() {
    for (const b of ivButtons) b.setAttribute('aria-pressed', String(Number(b.dataset.iv) === S.interval));
  }
  document.getElementById('intervals').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-iv]');
    if (!b) return;
    S.interval = Number(b.dataset.iv);
    store.set('interval', S.interval);
    syncIntervals();
    S.candles = [];
    S.candleSym = null;
    drawChart();
    loadCandles();
  });

  $('theme').addEventListener('click', () => {
    const root = document.documentElement;
    const dark = root.dataset.theme ? root.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
    root.dataset.theme = dark ? 'light' : 'dark';
    store.set('theme', root.dataset.theme);
    drawChart();
  });

  document.addEventListener('keydown', (e) => {
    const typing = e.target instanceof HTMLInputElement;
    if (e.key === '/' && !typing) { e.preventDefault(); search.focus(); search.select(); return; }
    if (e.key === 'Escape' && typing) { search.value = ''; search.dispatchEvent(new Event('input')); search.blur(); return; }
    if ((e.key === 'ArrowDown' || e.key === 'ArrowUp') && S.order.length) {
      e.preventDefault();
      const i = S.order.indexOf(S.selected);
      const next = i < 0 ? 0 : Math.min(S.order.length - 1, Math.max(0, i + (e.key === 'ArrowDown' ? 1 : -1)));
      select(S.order[next]);
    }
  });

  // ---------- timers ----------
  setInterval(headerStats, 1000);
  setInterval(updateAge, 1000);
  setInterval(() => { // idle badges and range positions age without new ticks
    for (const q of S.quotes.values()) if (q.el) updateRow(q, false);
    if (S.sort.key !== 'symbol') S.orderDirty = true;
    schedule();
  }, 15000);
  setInterval(() => { if (S.sort.key !== 'symbol') { S.orderDirty = true; schedule(); } }, 2000);
  setInterval(loadCandles, 60000); // resync with the engine (late ticks, expiry)
  setInterval(pollMetrics, 2000);

  syncIntervals();
  setStatus();
  updateDetail(false);
  loadCandles();
  pollMetrics();
  connect();
})();
