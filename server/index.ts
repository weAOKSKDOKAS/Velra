import express from 'express';
import path from 'path';
import { generateMarketSnapshot } from './generator.js'; // Note: .js extension for NodeNext resolution
import { uploadSnapshot, downloadSnapshot, checkFileExists } from './storage.js';
import { Region, Sector, MarketSnapshot } from '../types.js';

const app = express();
app.use(express.json() as any);

const PORT = process.env.PORT || 8080;
const SECRET = process.env.GENERATE_SECRET || 'velra-secret-key';

// --- MIDDLEWARE ---
app.use((req, res, next) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} | IP: ${ip} | UA: ${req.get('user-agent')} | Query: ${JSON.stringify(req.query)}`);
  next();
});

// CORS for Dev and Scheduler
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*"); 
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, x-velra-secret");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === 'OPTIONS') {
    res.sendStatus(200);
    return;
  }
  next();
});

// --- MAPPINGS ---
const mapRegionInput = (input: string): Region => {
  if (!input) return Region.INDONESIA;
  const norm = input.toLowerCase().trim();
  if (norm === 'us' || norm === 'amerika serikat' || norm === 'united states' || norm === 'usa') return Region.US;
  if (norm === 'am' || norm === 'amerika' || norm === 'americas') return Region.AMERICAS;
  if (norm === 'asia' || norm === 'apac') return Region.ASIA;
  if (norm === 'eu' || norm === 'europe' || norm === 'eropa') return Region.EUROPE;
  return Region.INDONESIA; 
};

const mapSectorInput = (input: string): Sector => {
  if (!input) return Sector.ALL;
  const norm = input.toLowerCase().trim();
  if (norm === 'all' || norm === 'umum') return Sector.ALL;
  if (norm === 'tech' || norm.includes('tekno') || norm.includes('technology')) return Sector.TECHNOLOGY;
  if (norm === 'fin' || norm.includes('keuangan') || norm.includes('finance')) return Sector.FINANCE;
  if (norm === 'mining' || norm.includes('energi') || norm.includes('tambang') || norm.includes('energy')) return Sector.MINING;
  if (norm === 'health' || norm.includes('kesehatan') || norm.includes('healthcare')) return Sector.HEALTHCARE;
  if (norm === 'reg' || norm.includes('regulasi') || norm.includes('regulation')) return Sector.REGULATION;
  if (norm === 'cons' || norm.includes('konsumsi') || norm.includes('consumer')) return Sector.CONSUMER;
  return Sector.ALL; 
};

// --- API ENDPOINTS (MUST BE BEFORE STATIC SERVING) ---

// 0. HEALTH CHECK & DIAGNOSTICS
app.get('/internal/health', (req, res) => {
  res.status(200).json({ ok: true, service: 'velra-terminal', timestamp: new Date().toISOString() });
});

app.get('/internal/diag', async (req, res) => {
  const hasKey = !!(process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || process.env.API_KEY);
  let bucketStatus = 'unknown';
  let writeStatus = 'unknown';
  
  try {
     const exists = await checkFileExists(Region.INDONESIA, Sector.ALL);
     bucketStatus = exists ? 'connected_found_data' : 'connected_no_data';
  } catch(e: any) { bucketStatus = `error: ${e.message}`; }

  res.json({
    ok: true,
    env: { hasApiKey: hasKey, bucket: process.env.GCS_BUCKET_NAME || 'default' },
    storage: { read: bucketStatus },
    timestamp: new Date().toISOString()
  });
});

// 1. READ PROXY
app.get('/api/snapshot', async (req, res) => {
  try {
    const region = mapRegionInput(req.query.region as string);
    const sector = mapSectorInput(req.query.sector as string);
    
    console.log(`[Read] Requesting: ${region} - ${sector}`);

    let data = await downloadSnapshot(region, sector);
    let fallbackType = null;

    if (!data && sector !== Sector.ALL) {
      console.log(`[Read] ${region}/${sector} missing. Fallback -> ${region}/ALL`);
      data = await downloadSnapshot(region, Sector.ALL);
      fallbackType = 'REGION_ALL';
    }

    if (!data && region !== Region.INDONESIA) {
      console.log(`[Read] ${region}/ALL missing. Fallback -> ID/ALL`);
      data = await downloadSnapshot(Region.INDONESIA, Sector.ALL);
      fallbackType = 'GLOBAL_ALL';
    }

    if (!data) {
      // In-Memory Fallback to prevent 404 on fresh boot
      console.warn(`[Read] CRITICAL: No data found in storage for ${region}. Returning empty shell.`);
      data = {
          metadata: { 
            generatedAt: new Date().toISOString(), 
            region, 
            sector, 
            versionId: 'fallback-memory-v0' 
          },
          outlook: { data: null, sources: [] },
          news: { items: [], sources: [] },
          indices: { indices: [], sources: [] },
          economics: { items: [], sources: [] }
      };
      fallbackType = 'MEMORY_ZERO';
    }

    if (fallbackType && data?.metadata) {
        data.metadata.versionId = `${data.metadata.versionId}-fallback-${fallbackType}`;
    }
    
    // Log what version we are serving to help debug "stale data" issues
    console.log(`[Read] Serving Version: ${data.metadata.versionId} (Generated: ${data.metadata.generatedAt})`);

    res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.json(data);
  } catch (error: any) {
    console.error("[Read] Error:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// 2. SEED MATRIX
app.post('/internal/seed-matrix', async (req, res) => {
  const authHeader = req.headers['x-velra-secret'];
  const querySecret = req.query.secret as string;
  if ((authHeader !== SECRET) && (querySecret !== SECRET)) return res.status(403).json({ error: 'Unauthorized' });

  const regions = Object.values(Region);
  const sectors = Object.values(Sector);
  const results = [];

  for (const r of regions) {
    for (const s of sectors) {
      if (!(await checkFileExists(r, s))) {
         console.log(`[Matrix] Seeding ${r}-${s}`);
         const seed: MarketSnapshot = {
            metadata: { generatedAt: new Date().toISOString(), region: r, sector: s, versionId: `seed-${Date.now()}` },
            outlook: { data: { type: s === Sector.ALL ? 'BRIEFING' : 'OUTLOOK', date: new Date().toLocaleDateString(), marketOverview: 'Waiting for AI generation.', topNews: [], watchlist: [], recommendation: { verdict: 'NEUTRAL', analysis: 'No data.' } }, sources: [] },
            news: { items: [], sources: [] },
            indices: { indices: [], sources: [] },
            economics: { items: [], sources: [] }
         };
         await uploadSnapshot(r, s, seed);
         results.push(`${r}-${s}`);
      }
    }
  }
  res.json({ status: 'success', created: results });
});

// 3. GENERATE SINGLE
app.post('/internal/generate', async (req, res) => {
  const authHeader = req.headers['x-velra-secret'];
  const querySecret = req.query.secret as string;
  if ((authHeader !== SECRET) && (querySecret !== SECRET)) return res.status(403).json({ error: 'Unauthorized' });

  try {
    const region = mapRegionInput((req.query.region || req.body.region) as string);
    const sector = mapSectorInput((req.query.sector || req.body.sector) as string);
    console.log(`[Gen] Single: ${region} (${sector})`);
    
    const snapshot = await generateMarketSnapshot(region, sector);
    const url = await uploadSnapshot(region, sector, snapshot);
    res.json({ status: 'success', url, version: snapshot.metadata.versionId });
  } catch (e: any) {
    console.error(`[Gen] Error: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// 4. GENERATE ALL (Bulk)
app.post('/internal/generateAll', async (req, res) => {
  const authHeader = req.headers['x-velra-secret'];
  const querySecret = req.query.secret as string;
  if ((authHeader !== SECRET) && (querySecret !== SECRET)) return res.status(403).json({ error: 'Unauthorized' });

  try {
    const region = mapRegionInput((req.query.region || req.body.region) as string);
    console.log(`[Gen] Bulk Starting for ${region}`);
    
    const sectors = Object.values(Sector);
    const results = [];
    
    // Process strictly sequentially to avoid API Rate Limits
    for (const s of sectors) {
      try {
        console.log(`[Gen] Processing ${s}...`);
        const snapshot = await generateMarketSnapshot(region, s);
        await uploadSnapshot(region, s, snapshot);
        results.push({ sector: s, status: 'ok' });
      } catch (e: any) {
        console.error(`[Gen] Error ${s}:`, e.message);
        results.push({ sector: s, status: 'error', msg: e.message });
      }
    }
    res.json({ status: 'bulk_complete', region, results });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// --- FRONTEND STATIC SERVING ---
// Robust path resolution for Docker/Cloud Run environments
const DIST_PATH = path.resolve('dist');

// Serve static assets
app.use(express.static(DIST_PATH));

// Handle React Routing (SPA Fallback)
app.get('*', (req, res) => {
  res.sendFile(path.join(DIST_PATH, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Velra Terminal (Full Stack) listening on port ${PORT}`);
  console.log(`Serving static files from: ${DIST_PATH}`);
});