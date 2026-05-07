import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import axios from 'axios';
import rateLimit from 'express-rate-limit';

// ==============================
// CONFIG
// ==============================
const LOAN_CORE_SERVERS = (
  process.env.LOAN_CORE_SERVERS ||
  'http://localhost:3001,http://localhost:3002'
).split(',');

const AUDIT_URL = process.env.AUDIT_URL || 'http://localhost:3010';
const PORT = Number(process.env.PORT || 3000);

// ==============================
// APP INIT
// ==============================
const app = express();
app.use(cors());
app.use(bodyParser.json());

// ==============================
// SIMPLE LOGGER
// ==============================
const log = (msg: string, meta?: any) => {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    service: 'api-gateway',
    message: msg,
    ...meta
  }));
};

// ==============================
// RATE LIMITING
// ==============================
// Global limiter: berlaku untuk SEMUA endpoint
const globalLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 menit
  max: 30,             // maks 30 request per IP per menit
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    error: 'Too many requests',
    message: 'Batas request tercapai. Coba lagi dalam 1 menit.',
    retryAfter: 60
  },
  handler: (req, res, _next, options) => {
    log('Rate limit exceeded', { ip: req.ip, path: req.path });
    res.status(429).json(options.message);
  }
});

// Loan Apply limiter: lebih ketat, khusus endpoint pengajuan pinjaman
const loanApplyLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 menit
  max: 5,              // maks 5 pengajuan loan per IP per menit
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    error: 'Too many loan requests',
    message: 'Terlalu banyak pengajuan pinjaman. Coba lagi dalam 1 menit.',
    retryAfter: 60
  },
  handler: (req, res, _next, options) => {
    log('Loan apply rate limit exceeded', { ip: req.ip });
    res.status(429).json(options.message);
  }
});

app.use(globalLimiter);                        // apply global ke semua route
app.use('/api/loans/apply', loanApplyLimiter); // apply ekstra limit ke loan apply



// ==============================
// LOAD BALANCER (ROUND ROBIN)
// ==============================
let loanIndex = 0;

function getLoanService() {
  const url = LOAN_CORE_SERVERS[loanIndex];
  loanIndex = (loanIndex + 1) % LOAN_CORE_SERVERS.length;
  return url;
}

// ==============================
// HEALTH CHECK (MULTI INSTANCE)
// ==============================
app.get('/health', async (_req, res) => {
  try {
    const loanChecks = await Promise.all(
      LOAN_CORE_SERVERS.map(url =>
        axios.get(url + '/loans/health', { timeout: 2000 })
          .then(r => ({ url, status: r.data }))
          .catch(() => ({ url, status: 'down' }))
      )
    );

    const audit = await axios
      .get(AUDIT_URL + '/health', { timeout: 2000 })
      .catch(() => null);

    res.json({
      status: 'ok',
      loanInstances: loanChecks,
      audit: audit?.data || 'unavailable'
    });

  } catch (err) {
    res.status(500).json({ status: 'error', error: String(err) });
  }
});

// ==============================
// LOAN APPLY (WITH LOAD BALANCING)
// ==============================
app.post('/api/loans/apply', async (req: Request, res: Response) => {
  const payload = req.body;
  const target = getLoanService();

  try {
    log('Forwarding loan request', { target });

    const r = await axios.post(
      target + '/loans/apply',
      payload,
      { timeout: 60000 }
    );

    res.json(r.data);

  } catch (err: any) {
    log('Loan service error', { target, error: err?.toString() });

    res.status(500).json({
      error: err?.toString(),
      target,
      details: err?.response?.data || null
    });
  }
});

// ==============================
// AUDIT SERVICE (NO LB)
// ==============================
app.get('/api/audit/:id', async (req, res) => {
  const id = req.params.id;

  try {
    const r = await axios.get(
      `${AUDIT_URL}/audit/${encodeURIComponent(id)}`,
      { timeout: 5000 }
    );

    res.json(r.data);

  } catch (err: any) {
    if (err.response?.status === 404) {
      return res.status(404).json({ error: 'not found' });
    }

    res.status(500).json({ error: String(err) });
  }
});

// ==============================
// START SERVER
// ==============================
const server = app.listen(PORT, () => {
  log('API Gateway started', {
    port: PORT,
    loanServices: LOAN_CORE_SERVERS,
    auditService: AUDIT_URL
  });
});

// ==============================
// HANDLE STARTUP ERROR
// ==============================
server.on('error', (err) => {
  log('Startup error', { error: err });
  process.exit(1);
});

// ==============================
// GRACEFUL SHUTDOWN
// ==============================
const shutdown = (signal: string) => {
  log('Shutdown signal received', { signal });

  server.close(() => {
    log('Server closed gracefully');
    process.exit(0);
  });

  setTimeout(() => {
    log('Force shutdown');
    process.exit(1);
  }, 5000);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);