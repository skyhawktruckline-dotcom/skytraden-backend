require('dotenv').config();
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const cron = require('node-cron');
const yahooFinance = require('yahoo-finance2').default;
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const FRONTEND_URL = process.env.FRONTEND_URL || '*';

// CORS - allow GoDaddy frontend
app.use(cors({
  origin: FRONTEND_URL,
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type']
}));
app.use(express.json());

// ===== IN-MEMORY CACHE =====
const cache = {
  quotes: {},      // symbol -> { price, change, changePercent, timestamp }
  candles: {},     // symbol -> { tf, data[], timestamp }
  search: {},      // query -> { results[], timestamp }
  profile: {}      // symbol -> { data, timestamp }
};
const CACHE_TTL = 5000; // 5 seconds for quotes
const CANDLE_TTL = 60000; // 60 seconds for candles

// ===== DEFAULT WATCHLIST =====
const DEFAULT_SYMS = [
  'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
  'META', 'NVDA', 'SPY', 'QQQ', 'IWM',
  'AMD', 'NFLX', 'BABA', 'UBER', 'COIN',
  'PLTR', 'SOFI', 'RIVN', 'LCID', 'NIO'
];

// ===== MAJOR INDEXES =====
const INDEXES = {
  '^GSPC': 'S&P 500',
  '^DJI': 'Dow Jones',
  '^IXIC': 'NASDAQ',
  '^RUT': 'Russell 2000',
  '^VIX': 'VIX'
};

// ===== WEBSOCKET CLIENTS =====
const clients = new Set();

// ===== FETCH REAL-TIME QUOTES =====
async function fetchQuotes(symbols) {
  const now = Date.now();
  const results = {};
  const toFetch = [];

  for (const sym of symbols) {
    const cached = cache.quotes[sym];
    if (cached && (now - cached.timestamp) < CACHE_TTL) {
      results[sym] = cached;
    } else {
      toFetch.push(sym);
    }
  }

  if (toFetch.length === 0) return results;

  try {
    const data = await yahooFinance.quote(toFetch);
    const quotes = Array.isArray(data) ? data : [data];

    for (const q of quotes) {
      const sym = q.symbol;
      const price = q.regularMarketPrice || q.price || 0;
      const prev = q.regularMarketPreviousClose || q.previousClose || price;
      const change = price - prev;
      const changePct = prev ? (change / prev) * 100 : 0;

      const entry = {
        sym,
        price: Math.round(price * 100) / 100,
        change: Math.round(change * 100) / 100,
        changePercent: Math.round(changePct * 100) / 100,
        volume: q.regularMarketVolume || q.volume || 0,
        high: q.regularMarketDayHigh || q.dayHigh || price,
        low: q.regularMarketDayLow || q.dayLow || price,
        open: q.regularMarketOpen || q.open || price,
        timestamp: now
      };
      cache.quotes[sym] = entry;
      results[sym] = entry;
    }
  } catch (e) {
    console.error('Quote fetch error:', e.message);
    // Return stale cache if available
    for (const sym of toFetch) {
      if (cache.quotes[sym]) results[sym] = cache.quotes[sym];
    }
  }

  return results;
}

// ===== FETCH HISTORICAL CANDLES =====
async function fetchCandles(sym, tf) {
  const cacheKey = `${sym}_${tf}`;
  const now = Date.now();
  const cached = cache.candles[cacheKey];
  if (cached && (now - cached.timestamp) < CANDLE_TTL) {
    return cached.data;
  }

  const periodMap = {
    '1m': { period1: new Date(Date.now() - 24 * 60 * 60 * 1000), interval: '1m' },
    '5m': { period1: new Date(Date.now() - 5 * 24 * 60 * 60 * 1000), interval: '5m' },
    '15m': { period1: new Date(Date.now() - 15 * 24 * 60 * 60 * 1000), interval: '15m' },
    '30m': { period1: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000), interval: '30m' },
    '1H': { period1: new Date(Date.now() - 60 * 24 * 60 * 60 * 1000), interval: '1h' },
    '4H': { period1: new Date(Date.now() - 120 * 24 * 60 * 60 * 1000), interval: '4h' },
    '1D': { period1: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000), interval: '1d' },
    '1W': { period1: new Date(Date.now() - 3 * 365 * 24 * 60 * 60 * 1000), interval: '1wk' },
    '1M': { period1: new Date(Date.now() - 10 * 365 * 24 * 60 * 60 * 1000), interval: '1mo' }
  };

  const config = periodMap[tf] || periodMap['15m'];

  try {
    const data = await yahooFinance.chart(sym, {
      period1: Math.floor(config.period1.getTime() / 1000),
      interval: config.interval
    });

    const quotes = data?.quotes || [];
    const candles = quotes.map(q => ({
      date: q.date ? new Date(q.date).getTime() : Date.now(),
      open: Math.round((q.open || 0) * 100) / 100,
      high: Math.round((q.high || 0) * 100) / 100,
      low: Math.round((q.low || 0) * 100) / 100,
      close: Math.round((q.close || 0) * 100) / 100,
      volume: q.volume || 0
    })).filter(c => c.close > 0);

    cache.candles[cacheKey] = { data: candles, timestamp: now };
    return candles;
  } catch (e) {
    console.error('Candle fetch error:', e.message);
    return cached?.data || [];
  }
}

// ===== SEARCH SYMBOLS =====
async function searchSymbols(query) {
  if (!query || query.length < 1) return [];

  const cacheKey = query.toLowerCase();
  const now = Date.now();
  const cached = cache.search[cacheKey];
  if (cached && (now - cached.timestamp) < 60000) {
    return cached.results;
  }

  try {
    const data = await yahooFinance.search(query, { quotesCount: 10, newsCount: 0 });
    const results = (data?.quotes || []).map(q => ({
      sym: q.symbol,
      name: q.shortname || q.longname || q.symbol,
      exchange: q.exchange || '',
      type: q.quoteType || 'EQUITY'
    }));

    cache.search[cacheKey] = { results, timestamp: now };
    return results;
  } catch (e) {
    console.error('Search error:', e.message);
    return cached?.results || [];
  }
}

// ===== FETCH PROFILE =====
async function fetchProfile(sym) {
  const now = Date.now();
  const cached = cache.profile[sym];
  if (cached && (now - cached.timestamp) < 300000) {
    return cached.data;
  }

  try {
    const data = await yahooFinance.quoteSummary(sym, { modules: ['assetProfile', 'financialData', 'defaultKeyStatistics'] });
    const profile = {
      name: data?.assetProfile?.name || sym,
      sector: data?.assetProfile?.sector || '',
      industry: data?.assetProfile?.industry || '',
      employees: data?.assetProfile?.fullTimeEmployees || 0,
      website: data?.assetProfile?.website || '',
      description: data?.assetProfile?.longBusinessSummary || '',
      marketCap: data?.defaultKeyStatistics?.marketCap || 0,
      pe: data?.defaultKeyStatistics?.trailingPE || 0,
      eps: data?.defaultKeyStatistics?.trailingEps || 0,
      dividend: data?.defaultKeyStatistics?.dividendRate || 0,
      beta: data?.defaultKeyStatistics?.beta || 0,
      fiftyTwoWeekHigh: data?.defaultKeyStatistics?.fiftyTwoWeekHigh || 0,
      fiftyTwoWeekLow: data?.defaultKeyStatistics?.fiftyTwoWeekLow || 0,
      avgVolume: data?.defaultKeyStatistics?.averageVolume || 0
    };

    cache.profile[sym] = { data: profile, timestamp: now };
    return profile;
  } catch (e) {
    console.error('Profile fetch error:', e.message);
    return cached?.data || { name: sym };
  }
}

// ===== BROADCAST TO ALL WS CLIENTS =====
function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

// ===== REST ENDPOINTS =====

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: Date.now(), connectedClients: clients.size });
});

// Get quotes for symbols
app.get('/api/quotes', async (req, res) => {
  const symbols = req.query.symbols?.split(',') || DEFAULT_SYMS;
  const quotes = await fetchQuotes(symbols);
  res.json(quotes);
});

// Get single quote
app.get('/api/quote/:symbol', async (req, res) => {
  const quotes = await fetchQuotes([req.params.symbol.toUpperCase()]);
  res.json(quotes[req.params.symbol.toUpperCase()] || null);
});

// Get historical candles
app.get('/api/candles/:symbol', async (req, res) => {
  const sym = req.params.symbol.toUpperCase();
  const tf = req.query.tf || '15m';
  const candles = await fetchCandles(sym, tf);
  res.json({ sym, tf, candles });
});

// Search symbols
app.get('/api/search', async (req, res) => {
  const query = req.query.q || '';
  const results = await searchSymbols(query);
  res.json(results);
});

// Get profile
app.get('/api/profile/:symbol', async (req, res) => {
  const sym = req.params.symbol.toUpperCase();
  const profile = await fetchProfile(sym);
  res.json(profile);
});

// Get major indexes
app.get('/api/indexes', async (req, res) => {
  const symbols = Object.keys(INDEXES);
  const quotes = await fetchQuotes(symbols);
  const results = symbols.map(sym => ({
    sym,
    name: INDEXES[sym],
    ...quotes[sym]
  }));
  res.json(results);
});

// Get news (placeholder - Yahoo search doesn't have news in v2 easily)
app.get('/api/news/:symbol', async (req, res) => {
  res.json([]);
});

// ===== WEBSOCKET =====
wss.on('connection', (ws) => {
  console.log('WS client connected');
  clients.add(ws);
  ws.subscriptions = new Set();

  ws.on('message', async (msg) => {
    try {
      const data = JSON.parse(msg);

      if (data.action === 'subscribe') {
        const syms = Array.isArray(data.symbols) ? data.symbols : [data.symbols];
        syms.forEach(s => ws.subscriptions.add(s.toUpperCase()));
        // Send immediate snapshot
        const quotes = await fetchQuotes([...ws.subscriptions]);
        ws.send(JSON.stringify({ type: 'snapshot', quotes }));
      }

      if (data.action === 'unsubscribe') {
        const syms = Array.isArray(data.symbols) ? data.symbols : [data.symbols];
        syms.forEach(s => ws.subscriptions.delete(s.toUpperCase()));
      }

      if (data.action === 'candles') {
        const candles = await fetchCandles(data.symbol.toUpperCase(), data.tf || '15m');
        ws.send(JSON.stringify({ type: 'candles', symbol: data.symbol, tf: data.tf, candles }));
      }
    } catch (e) {
      console.error('WS message error:', e.message);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log('WS client disconnected');
  });
});

// ===== BACKGROUND PRICE STREAM =====
let lastBroadcast = {};

async function broadcastPrices() {
  const allSymbols = new Set(DEFAULT_SYMS);
  clients.forEach(c => c.subscriptions.forEach(s => allSymbols.add(s)));

  if (allSymbols.size === 0) return;

  const quotes = await fetchQuotes([...allSymbols]);
  const changes = {};

  for (const [sym, q] of Object.entries(quotes)) {
    const last = lastBroadcast[sym];
    if (!last || Math.abs(last.price - q.price) > 0.001 || last.change !== q.change) {
      changes[sym] = q;
      lastBroadcast[sym] = q;
    }
  }

  if (Object.keys(changes).length > 0) {
    broadcast({ type: 'prices', quotes: changes, time: Date.now() });
  }
}

// Stream every 3 seconds during market hours
cron.schedule('*/3 * * * * *', () => {
  broadcastPrices();
});

// Pre-warm cache on startup
(async () => {
  console.log('Pre-warming cache...');
  await fetchQuotes(DEFAULT_SYMS);
  await fetchQuotes(Object.keys(INDEXES));
  console.log('Cache ready');
})();

// ===== START SERVER =====
server.listen(PORT, () => {
  console.log(`SkyTraden Backend running on port ${PORT}`);
  console.log(`Frontend CORS: ${FRONTEND_URL}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
});
