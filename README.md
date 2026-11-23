# SkyTraden Backend

Real-time stock data backend powered by Yahoo Finance. Serves live quotes, historical candles, and symbol search via REST API and WebSocket.

## Deploy to Railway

1. **Push to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/skytraden-backend.git
   git push -u origin main
   ```

2. **Create Railway Project**
   - Go to https://railway.app
   - Click "New Project" → "Deploy from GitHub repo"
   - Select your repo
   - Railway auto-detects the Dockerfile

3. **Set Environment Variables**
   - In Railway dashboard → Variables
   - Add: `FRONTEND_URL=https://skytraden.com`
   - Add: `PORT=3000`

4. **Get Domain**
   - Railway auto-generates a domain like `skytraden-backend.up.railway.app`
   - Copy this URL — you need it for the frontend

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Health check |
| `GET /api/quotes?symbols=AAPL,MSFT` | Get quotes for multiple symbols |
| `GET /api/quote/AAPL` | Get single quote |
| `GET /api/candles/AAPL?tf=15m` | Historical candles (1m,5m,15m,30m,1H,4H,1D,1W,1M) |
| `GET /api/search?q=apple` | Symbol search |
| `GET /api/profile/AAPL` | Company profile |
| `GET /api/indexes` | Major indexes (S&P 500, Dow, NASDAQ, etc.) |
| `WS /` | WebSocket for real-time streaming |

## WebSocket Protocol

```javascript
// Subscribe to symbols
ws.send(JSON.stringify({
  action: 'subscribe',
  symbols: ['AAPL', 'MSFT', 'TSLA']
}));

// Get candles
ws.send(JSON.stringify({
  action: 'candles',
  symbol: 'AAPL',
  tf: '15m'
}));

// Received messages
{ type: 'snapshot', quotes: { AAPL: {...}, MSFT: {...} } }
{ type: 'prices', quotes: { AAPL: {...} }, time: 1234567890 }
{ type: 'candles', symbol: 'AAPL', tf: '15m', candles: [...] }
```
