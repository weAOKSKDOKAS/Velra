import express from 'express';
import { Storage } from '@google-cloud/storage';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

// --- CONFIGURATION ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 8080;
const BUCKET_NAME = process.env.GCS_BUCKET;
const DATA_OBJECT = process.env.DATA_OBJECT || 'data.json';

// Initialize Storage
const storage = new Storage();

// --- STARTUP CHECKS ---
const INDEX_PATH = path.join(__dirname, 'index.html');
const HAS_UI = fs.existsSync(INDEX_PATH);

console.log(`[Startup] Environment: ${process.env.NODE_ENV || 'development'}`);
console.log(`[Startup] GCS Bucket: ${BUCKET_NAME || 'NOT SET (Will use fallback)'}`);
console.log(`[Startup] GCS Object: ${DATA_OBJECT}`);
console.log(`[Startup] UI Check: ${HAS_UI ? 'FOUND' : 'MISSING'} at ${INDEX_PATH}`);

// --- FALLBACK DATA (Safe Schema to prevent UI crashes) ---
const FALLBACK_DATA = {
  schema_version: 2,
  generatedAt: new Date().toISOString(),
  generated_at_wib: new Intl.DateTimeFormat("id-ID",{ timeZone:"Asia/Jakarta", dateStyle:"medium", timeStyle:"short" }).format(new Date()),
  status: { ok: false, last_error: "Snapshot belum tersedia (jalankan Cloud Run Job writer).", last_success_at_wib: null },
  indices: { INDONESIA: [], USA: [], ASIA: [], EUROPE: [] },
  indicators: { INDONESIA: [], USA: [], ASIA: [], EUROPE: [] },
  livewire: [],
  briefings: { regions: { INDONESIA: {}, USA: {}, ASIA: {}, EUROPE: {} } },
  morningBriefs: {},
  mbMeta: {}
};

// --- MIDDLEWARE ---
app.use((req, res, next) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} | IP: ${ip} | UA: ${req.get('user-agent')}`);
  next();
});

// --- API ROUTES (Priority over Static) ---

// 1. Health Checks
app.get(['/health', '/internal/health'], (req, res) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

// 2. Diagnostics (Proof of Pipeline)
app.get('/internal/diag', async (req, res) => {
  if (!BUCKET_NAME) return res.status(500).json({ error: "GCS_BUCKET missing" });
  
  try {
    const bucket = storage.bucket(BUCKET_NAME);
    const file = bucket.file(DATA_OBJECT);
    const [exists] = await file.exists();
    
    if (!exists) {
        return res.json({ ok: true, status: 'missing_file', bucket: BUCKET_NAME });
    }
    
    const [metadata] = await file.getMetadata();
    res.json({
        ok: true,
        bucket: BUCKET_NAME,
        gcs_object_updated_at: metadata.updated,
        gcs_generation: metadata.generation,
        size: metadata.size,
        served_at: new Date().toISOString()
    });
  } catch(e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 3. Data Proxy
app.get('/data.json', async (req, res) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Content-Type', 'application/json');

  if (!BUCKET_NAME) {
    console.error("GCS_BUCKET environment variable missing");
    return res.json(FALLBACK_DATA);
  }

  try {
    const bucket = storage.bucket(BUCKET_NAME);
    const file = bucket.file(DATA_OBJECT);
    const [exists] = await file.exists();

    if (!exists) {
      console.warn("data.json not found in bucket, serving fallback.");
      return res.json(FALLBACK_DATA);
    }

    // Step 2 Requirement: Inject Debug Headers
    const [metadata] = await file.getMetadata();
    res.set('X-GCS-Updated-At', metadata.updated);
    res.set('X-GCS-Generation', metadata.generation);

    const [content] = await file.download();
    const data = JSON.parse(content.toString());
    
    // Integrity check
    if (!data.briefings || !data.indices) throw new Error("Invalid JSON structure");

    res.json(data);
  } catch (error) {
    console.error("Error reading from GCS:", error);
    const errData = { ...FALLBACK_DATA };
    errData.status.last_error = error.message;
    res.json(errData);
  }
});

// --- STATIC ROUTES (Frontend) ---

if (HAS_UI) {
  // Serve static assets from root directory (css, js, images if any)
  app.use(express.static(__dirname));

  // Serve index.html at root
  app.get('/', (req, res) => {
    res.sendFile(INDEX_PATH);
  });

  // SPA Fallback: Any other route returns index.html (unless it matches an API route above)
  app.get('*', (req, res) => {
    // Safety check: ensure we don't return HTML for API calls that might have missed
    if (req.path.startsWith('/data.json') || req.path.startsWith('/internal')) {
      return res.status(404).json({ error: "Not Found" });
    }
    res.sendFile(INDEX_PATH);
  });
} else {
  // Graceful failure if index.html is missing
  app.get('/', (req, res) => {
    res.status(500).send(`
      <html>
        <body style="background:#111; color:#fff; font-family:monospace; padding:2rem;">
          <h1>Server Error</h1>
          <p>The static UI file (index.html) was not found in the server directory.</p>
          <p>Path checked: ${INDEX_PATH}</p>
        </body>
      </html>
    `);
  });
}

// --- SERVER START ---
app.listen(PORT, () => {
  console.log(`Velra Terminal Reader listening on port ${PORT}`);
});