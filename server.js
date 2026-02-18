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

// 4. OG-Image Proxy — extracts og:image from article URLs for wire card covers
const ogCache = new Map();
const OG_CACHE_TTL = 60 * 60 * 1000; // 1 hour
const OG_FETCH_TIMEOUT = 8000;

function extractOgImage(html) {
  if (!html) return "";
  // og:image (both attribute orders)
  const og = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i)
    || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i);
  if (og?.[1]) return og[1].trim();
  // twitter:image
  const tw = html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i)
    || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter:image["']/i);
  if (tw?.[1]) return tw[1].trim();
  return "";
}

app.get('/api/og-image', async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).json({ imageUrl: "" });

  // Check cache
  const cached = ogCache.get(url);
  if (cached && Date.now() - cached.ts < OG_CACHE_TTL) {
    return res.json({ imageUrl: cached.imageUrl });
  }

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), OG_FETCH_TIMEOUT);
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,*/*",
      },
      redirect: "follow",
    });
    clearTimeout(timer);

    if (!response.ok) throw new Error("Not OK");
    const ct = response.headers.get("content-type") || "";
    if (!ct.includes("html") && !ct.includes("text")) throw new Error("Not HTML");

    const html = await response.text();
    const imageUrl = extractOgImage(html);

    ogCache.set(url, { imageUrl, ts: Date.now() });
    res.json({ imageUrl });
  } catch {
    ogCache.set(url, { imageUrl: "", ts: Date.now() });
    res.json({ imageUrl: "" });
  }
});

// Batch endpoint: resolve multiple article URLs at once
app.post('/api/og-images', express.json(), async (req, res) => {
  const urls = Array.isArray(req.body?.urls) ? req.body.urls.slice(0, 30) : [];
  if (!urls.length) return res.json({ results: {} });

  const results = {};
  const toFetch = [];

  // Check cache first
  for (const url of urls) {
    const cached = ogCache.get(url);
    if (cached && Date.now() - cached.ts < OG_CACHE_TTL) {
      results[url] = cached.imageUrl;
    } else {
      toFetch.push(url);
    }
  }

  // Fetch uncached in parallel (max 5 concurrent)
  const BATCH_SIZE = 5;
  for (let i = 0; i < toFetch.length; i += BATCH_SIZE) {
    const batch = toFetch.slice(i, i + BATCH_SIZE);
    const settled = await Promise.allSettled(
      batch.map(async (url) => {
        try {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), OG_FETCH_TIMEOUT);
          const response = await fetch(url, {
            signal: controller.signal,
            headers: {
              "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
              "accept": "text/html,application/xhtml+xml,*/*",
            },
            redirect: "follow",
          });
          clearTimeout(timer);
          if (!response.ok) return { url, imageUrl: "" };
          const ct = response.headers.get("content-type") || "";
          if (!ct.includes("html") && !ct.includes("text")) return { url, imageUrl: "" };
          const html = await response.text();
          return { url, imageUrl: extractOgImage(html) };
        } catch {
          return { url, imageUrl: "" };
        }
      })
    );
    for (const r of settled) {
      const val = r.status === "fulfilled" ? r.value : { url: "", imageUrl: "" };
      if (val.url) {
        ogCache.set(val.url, { imageUrl: val.imageUrl, ts: Date.now() });
        results[val.url] = val.imageUrl;
      }
    }
  }

  res.json({ results });
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
    if (req.path.startsWith('/data.json') || req.path.startsWith('/internal') || req.path.startsWith('/api/')) {
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