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

const IBKR_BASE = 'https://localhost:5000/v1/api';
const IBKR_WS = 'wss://localhost:5000/v1/api/ws';

function ibkrFetch(path, opts = {}) {
  return fetch(IBKR_BASE + path, { ...opts, agent: new (require('https').Agent)({ rejectUnauthorized: false }) });
}

let ibkrConnected = false;
let ibkrWsClient = null;
let clients = new Set();

async function checkIBKR() {
  try {
    const res = await ibkrFetch('/iserver/auth/status', { timeout: 3000 });
    ibkrConnected = res.ok;
    return ibkrConnected;
  } catch(e) { ibkrConnected = false; return false; }
}

setInterval(() => checkIBKR(), 15000);
checkIBKR();

// REST endpoints
app.get('/api/status', async (req, res) => {
  const connected = await checkIBKR();
  res.json({ connected, time: new Date().toISOString() });
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

// WebSocket handling
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'status', connected: ibkrConnected }));
  
  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.type === 'subscribe' && data.conid) {
        if (ibkrConnected && ibkrWsClient && ibkrWsClient.readyState === WebSocket.OPEN) {
          ibkrWsClient.send(JSON.stringify({ conid: data.conid, reqType: 1 }));
        }
      }
    } catch (e) {}
  });
  
  ws.on('close', () => clients.delete(ws));
});

function broadcast(msg) {
  const str = JSON.stringify(msg);
  clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(str); });
}

function connectIBKRWS() {
  if (!ibkrConnected) { setTimeout(connectIBKRWS, 5000); return; }
  try {
    ibkrWsClient = new WebSocket(IBKR_WS, { rejectUnauthorized: false });
    ibkrWsClient.on('open', () => { console.log('IBKR WS connected'); });
    ibkrWsClient.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.conid) broadcast({ type: 'tick', ...msg });
      } catch (e) {}
    });
    ibkrWsClient.on('close', () => { setTimeout(connectIBKRWS, 10000); });
    ibkrWsClient.on('error', () => { ibkrWsClient.close(); });
  } catch (e) { setTimeout(connectIBKRWS, 10000); }
}
connectIBKRWS();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log('SkyTraden backend on port ' + PORT); });
