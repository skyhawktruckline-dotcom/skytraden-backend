const express = require('express');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');
const fetch = require('node-fetch');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

// Config - Massive (formerly Polygon.io)
const MASSIVE_API_KEY = process.env.MASSIVE_API_KEY || '';
const MASSIVE_REST = 'https://api.massive.com';
const MASSIVE_WS = 'wss://socket.massive.com';
const IBKR_BASE = 'https://localhost:5000/v1/api';

let dataSource = 'massive'; // 'massive' or 'ibkr'
let ibkrConnected = false;
let massiveConnected = false;
let clients = new Set();
let massiveWsClient = null;
let ibkrWsClient = null;

// Helper for IBKR fetch
function ibkrFetch(path, opts = {}) {
  return fetch(IBKR_BASE + path, { ...opts, agent: new (require('https').Agent)({ rejectUnauthorized: false }) });
}

// Check IBKR connection
async function checkIBKR() {
  try {
    const res = await ibkrFetch('/iserver/auth/status', { timeout: 3000 });
    ibkrConnected = res.ok;
    return ibkrConnected;
  } catch(e) { ibkrConnected = false; return false; }
}

// ==================== MASSIVE.IO INTEGRATION ====================

// Proxy Massive REST API
app.get('/api/massive/*', async (req, res) => {
  if (!MASSIVE_API_KEY) return res.status(503).json({ error: 'Massive API key not configured' });
  try {
    const path = req.params[0];
    const query = Object.entries(req.query).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join('&');
    const url = `${MASSIVE_REST}/${path}?${query}&apiKey=${MASSIVE_API_KEY}`;
    const resp = await fetch(url);
    const data = await resp.json();
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// Get aggregates (candlestick data) from Massive
app.get('/api/aggregates/:ticker/:multiplier/:timespan/:from/:to', async (req, res) => {
  if (!MASSIVE_API_KEY) return res.status(503).json({ error: 'Massive API key not configured. Add MASSIVE_API_KEY to Railway environment variables.' });
  try {
    const { ticker, multiplier, timespan, from, to } = req.params;
    const adjusted = req.query.adjusted !== 'false';
    const url = `${MASSIVE_REST}/v2/aggs/ticker/${encodeURIComponent(ticker)}/range/${multiplier}/${timespan}/${from}/${to}?adjusted=${adjusted}&sort=asc&apiKey=${MASSIVE_API_KEY}`;
    const resp = await fetch(url);
    const data = await resp.json();
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// Get last trade/quote
app.get('/api/last/:type/:ticker', async (req, res) => {
  if (!MASSIVE_API_KEY) return res.status(503).json({ error: 'Massive API key not configured' });
  try {
    const { type, ticker } = req.params;
    const endpoint = type === 'quote' ? 'last_quote' : 'last_trade';
    const url = `${MASSIVE_REST}/v2/${endpoint}/${encodeURIComponent(ticker)}?apiKey=${MASSIVE_API_KEY}`;
    const resp = await fetch(url);
    const data = await resp.json();
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// Get all tickers (search)
app.get('/api/tickers', async (req, res) => {
  if (!MASSIVE_API_KEY) return res.status(503).json({ error: 'Massive API key not configured' });
  try {
    const search = req.query.search || '';
    const market = req.query.market || 'fx';
    const url = `${MASSIVE_REST}/v3/reference/tickers?market=${market}&search=${encodeURIComponent(search)}&active=true&limit=50&apiKey=${MASSIVE_API_KEY}`;
    const resp = await fetch(url);
    const data = await resp.json();
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// ==================== IBKR ENDPOINTS (BACKUP) ====================

app.get('/api/status', async (req, res) => {
  const ibkr = await checkIBKR();
  res.json({ 
    connected: ibkr || !!MASSIVE_API_KEY, 
    massive: massiveConnected || !!MASSIVE_API_KEY,
    ibkr: ibkrConnected,
    dataSource: MASSIVE_API_KEY ? 'massive' : (ibkr ? 'ibkr' : 'none'),
    mode: massiveConnected ? 'realtime' : (MASSIVE_API_KEY ? 'delayed' : 'none'),
    message: massiveConnected ? 'Real-time WebSocket active' : (MASSIVE_API_KEY ? 'Delayed data active (free plan) - upgrade for real-time' : 'No data source configured'),
    time: new Date().toISOString() 
  });
});

app.get('/api/accounts', async (req, res) => {
  try {
    const r = await ibkrFetch('/iserver/accounts');
    if (!r.ok) throw new Error('Not authenticated');
    const data = await r.json();
    const accounts = (data.accounts || []).map(a => ({ accountId: a, accountTitle: a, currency: 'USD' }));
    res.json({ accounts });
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/positions', async (req, res) => {
  try {
    const r = await ibkrFetch('/portfolio/positions');
    if (!r.ok) throw new Error('Failed');
    const data = await r.json();
    res.json({ positions: data || [] });
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/orders', async (req, res) => {
  try {
    const r = await ibkrFetch('/iserver/account/orders');
    if (!r.ok) throw new Error('Failed');
    const data = await r.json();
    res.json({ orders: data.orders || [] });
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.post('/api/order', async (req, res) => {
  try {
    const { accountId, side, qty, symbol, type, price, tif } = req.body;
    const order = {
      orders: [{
        acctId: accountId,
        conid: symbol,
        orderType: type || 'MKT',
        side: side,
        quantity: qty,
        tif: tif || 'DAY',
        price: price || undefined
      }]
    };
    const r = await ibkrFetch('/iserver/account/' + accountId + '/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(order)
    });
    const data = await r.json();
    res.json({ success: true, order: data });
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/quote/:conid', async (req, res) => {
  try {
    const r = await ibkrFetch('/iserver/marketdata/snapshot?conids=' + req.params.conid);
    if (!r.ok) throw new Error('Failed');
    const data = await r.json();
    res.json(data[0] || {});
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/search/:symbol', async (req, res) => {
  try {
    const r = await ibkrFetch('/iserver/secdef/search?symbol=' + encodeURIComponent(req.params.symbol));
    if (!r.ok) throw new Error('Failed');
    const data = await r.json();
    res.json(data || []);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/copy-trading', (req, res) => {
  res.json({ status: 'ready', note: 'Configure allocation groups in IBKR Client Portal' });
});

app.get('/api/followers', (req, res) => {
  res.json({ followers: [] });
});

// ==================== WEBSOCKET HANDLING ====================

function broadcast(msg) {
  const str = JSON.stringify(msg);
  clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(str); });
}

wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'status', connected: ibkrConnected, massive: massiveConnected, dataSource: MASSIVE_API_KEY ? 'massive' : 'ibkr' }));
  
  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      // Forward Massive subscriptions
      if (data.type === 'subscribe' && data.channel && massiveWsClient && massiveWsClient.readyState === WebSocket.OPEN) {
        massiveWsClient.send(JSON.stringify({ action: 'subscribe', params: data.channel }));
      }
      // Forward IBKR subscriptions
      if (data.type === 'subscribe_ibkr' && data.conid && ibkrWsClient && ibkrWsClient.readyState === WebSocket.OPEN) {
        ibkrWsClient.send(JSON.stringify({ conid: data.conid, reqType: 1 }));
      }
    } catch (e) {}
  });
  
  ws.on('close', () => clients.delete(ws));
});

// ==================== MASSIVE REAL-TIME DATA ====================
// Priority: WebSocket (real-time, sub-second) → REST polling (fallback, 1-5 sec delay)

const massiveSubscriptions = new Set(['ES', 'NQ', 'CL', 'GC', 'SI', 'NG', 'YM', 'RTY']);
let pollingInterval = null;
let wsRetryCount = 0;
const MAX_WS_RETRY = 5;

// REST API polling using aggregates (works on free Massive plan)
// Free plan only allows historical aggregates, not last_trade/last_quote
async function pollMassive() {
  if (!MASSIVE_API_KEY || massiveConnected) return;
  for (const sym of massiveSubscriptions) {
    try {
      const ticker = `C:${sym}!1!`;
      const today = new Date().toISOString().split('T')[0];
      const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
      
      // Fetch latest 5-minute aggregate bar (free plan supports this)
      const aggUrl = `${MASSIVE_REST}/v2/aggs/ticker/${encodeURIComponent(ticker)}/range/5/minute/${yesterday}/${today}?adjusted=true&sort=desc&limit=1&apiKey=${MASSIVE_API_KEY}`;
      const aggResp = await fetch(aggUrl, { timeout: 5000 });
      if (aggResp.ok) {
        const agg = await aggResp.json();
        if (agg && agg.results && agg.results.length > 0) {
          const r = agg.results[0];
          broadcast({ 
            type: 'aggregate', 
            symbol: sym, 
            open: r.o, 
            high: r.h, 
            low: r.l, 
            close: r.c, 
            volume: r.v, 
            timestamp: r.t 
          });
          broadcast({ 
            type: 'tick', 
            symbol: sym, 
            price: r.c, 
            size: r.v, 
            timestamp: r.t 
          });
          broadcast({ 
            type: 'quote', 
            symbol: sym, 
            bid: r.l, 
            ask: r.h, 
            bidSize: Math.floor(r.v * 0.4), 
            askSize: Math.floor(r.v * 0.6), 
            timestamp: r.t 
          });
        }
      }
    } catch (e) { console.log('Poll error:', e.message); }
  }
}

function startPolling() {
  if (pollingInterval) clearInterval(pollingInterval);
  pollingInterval = setInterval(pollMassive, 2000); // 2 second fallback polling
}

function stopPolling() {
  if (pollingInterval) { clearInterval(pollingInterval); pollingInterval = null; }
}

// Connect to Massive WebSocket (REAL-TIME - requires paid plan)
function connectMassiveWS() {
  if (!MASSIVE_API_KEY) { console.log('No Massive API key configured'); return; }
  if (wsRetryCount >= MAX_WS_RETRY) {
    console.log('Max WebSocket retries reached. Using REST polling fallback (2s delay). Upgrade Massive plan for real-time.');
    startPolling();
    return;
  }
  try {
    massiveWsClient = new WebSocket(`${MASSIVE_WS}/futures`);
    massiveWsClient.on('open', () => {
      console.log('Massive WS connected - REAL-TIME MODE ACTIVE');
      massiveConnected = true;
      wsRetryCount = 0;
      stopPolling(); // Stop polling when WebSocket is live
      massiveWsClient.send(JSON.stringify({ action: 'auth', params: MASSIVE_API_KEY }));
      massiveWsClient.send(JSON.stringify({ action: 'subscribe', params: 'A.ES,T.ES,A.NQ,T.NQ,A.CL,T.CL,A.GC,T.GC,A.SI,T.SI,A.NG,T.NG,A.YM,T.YM,A.RTY,T.RTY' }));
    });
    massiveWsClient.on('message', (data) => {
      try {
        const msgs = JSON.parse(data);
        if (!Array.isArray(msgs)) return;
        msgs.forEach(msg => {
          if (msg.ev === 'A') { broadcast({ type: 'aggregate', symbol: msg.sym, open: msg.o, high: msg.h, low: msg.l, close: msg.c, volume: msg.v, timestamp: msg.t }); }
          else if (msg.ev === 'T') { broadcast({ type: 'tick', symbol: msg.sym, price: msg.p, size: msg.s, timestamp: msg.t }); }
          else if (msg.ev === 'Q') { broadcast({ type: 'quote', symbol: msg.sym, bid: msg.bp, ask: msg.ap, bidSize: msg.bs, askSize: msg.as, timestamp: msg.t }); }
        });
      } catch (e) {}
    });
    massiveWsClient.on('close', () => {
      massiveConnected = false;
      console.log('Massive WS disconnected. Retrying...');
      wsRetryCount++;
      if (wsRetryCount >= MAX_WS_RETRY) { startPolling(); }
      else { setTimeout(connectMassiveWS, 5000); }
    });
    massiveWsClient.on('error', (e) => {
      console.log('Massive WS error:', e.message);
      massiveConnected = false;
      wsRetryCount++;
      massiveWsClient.close();
    });
  } catch (e) {
    wsRetryCount++;
    if (wsRetryCount >= MAX_WS_RETRY) { startPolling(); }
    else { setTimeout(connectMassiveWS, 5000); }
  }
}

// Connect to IBKR WebSocket
function connectIBKRWS() {
  if (!ibkrConnected) { setTimeout(connectIBKRWS, 5000); return; }
  try {
    ibkrWsClient = new WebSocket('wss://localhost:5000/v1/api/ws', { rejectUnauthorized: false });
    ibkrWsClient.on('open', () => { console.log('IBKR WS connected'); });
    ibkrWsClient.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.conid) broadcast({ type: 'tick_ibkr', ...msg });
      } catch (e) {}
    });
    ibkrWsClient.on('close', () => { setTimeout(connectIBKRWS, 10000); });
    ibkrWsClient.on('error', () => { ibkrWsClient.close(); });
  } catch (e) { setTimeout(connectIBKRWS, 10000); }
}

// Periodic IBKR check
setInterval(() => checkIBKR(), 15000);
checkIBKR();

// Start connections
connectMassiveWS();
connectIBKRWS();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log('SkyTraden backend on port ' + PORT); });
