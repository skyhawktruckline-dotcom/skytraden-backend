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
    massive: massiveConnected,
    ibkr: ibkrConnected,
    dataSource: MASSIVE_API_KEY ? 'massive' : (ibkr ? 'ibkr' : 'none'),
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

// Connect to Massive WebSocket
function connectMassiveWS() {
  if (!MASSIVE_API_KEY) { console.log('No Massive API key configured'); return; }
  try {
    massiveWsClient = new WebSocket(`${MASSIVE_WS}/futures`);
    massiveWsClient.on('open', () => {
      console.log('Massive WS connected');
      massiveConnected = true;
      massiveWsClient.send(JSON.stringify({ action: 'auth', params: MASSIVE_API_KEY }));
      // Auto-subscribe to common futures
      massiveWsClient.send(JSON.stringify({ action: 'subscribe', params: 'A.ES,T.ES,A.NQ,T.NQ,A.CL,T.CL,A.GC,T.GC' }));
    });
    massiveWsClient.on('message', (data) => {
      try {
        const msgs = JSON.parse(data);
        if (!Array.isArray(msgs)) return;
        msgs.forEach(msg => {
          if (msg.ev === 'A') { // Aggregate (bar)
            broadcast({ type: 'aggregate', symbol: msg.sym, open: msg.o, high: msg.h, low: msg.l, close: msg.c, volume: msg.v, timestamp: msg.t });
          } else if (msg.ev === 'T') { // Trade
            broadcast({ type: 'tick', symbol: msg.sym, price: msg.p, size: msg.s, timestamp: msg.t });
          } else if (msg.ev === 'Q') { // Quote
            broadcast({ type: 'quote', symbol: msg.sym, bid: msg.bp, ask: msg.ap, bidSize: msg.bs, askSize: msg.as, timestamp: msg.t });
          }
        });
      } catch (e) {}
    });
    massiveWsClient.on('close', () => { massiveConnected = false; setTimeout(connectMassiveWS, 10000); });
    massiveWsClient.on('error', (e) => { console.log('Massive WS error:', e.message); massiveWsClient.close(); });
  } catch (e) { setTimeout(connectMassiveWS, 10000); }
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
