import { Storage } from "@google-cloud/storage";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { XMLParser } from "fast-xml-parser";
import { GoogleGenAI } from "@google/genai";

/**
 * Velra Terminal Writer (Cloud Run Job)
 *
 * Goals:
 * - RSS for discovery (cheap)
 * - Gemini for writing only (optional, bounded)
 * - No dummy news. Empty is valid.
 * - Persist news <= 24h by snapshot merge
 *
 * Output file (GCS): data.json (schema_version 2)
 */

// -------- ENV / CONFIG --------
const BUCKET = process.env.GCS_BUCKET || process.env.BUCKET_NAME || "";
const OBJECT = process.env.DATA_OBJECT || "data.json";

const RUN_MODE = String(process.env.MODE || "hourly").toLowerCase(); // hourly | morning | full
const LOOKBACK_HOURS = Number(process.env.LOOKBACK_HOURS || (RUN_MODE === "full" ? 24 : 1));
const TTL_HOURS = Number(process.env.TTL_HOURS || 24);
// How wide RSS discovery is. Keep this wide (default: TTL) so first run is not empty.
const RSS_WINDOW_HOURS = Number(process.env.RSS_WINDOW_HOURS || TTL_HOURS);

// Gemini (optional)
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || process.env.API_KEY || "";
const GEMINI_MODEL = process.env.GEMINI_MODEL || ""; // if empty, auto-fallback list
const GEMINI_MAX_NEW = Number(process.env.GEMINI_MAX_NEW || 14); // max stories rewritten per region per run
const GEMINI_MAX_TOKENS = Number(process.env.GEMINI_MAX_TOKENS || 2500);
const GEMINI_TEMPERATURE = Number(process.env.GEMINI_TEMPERATURE || 0.3);

// Limits
const SECTOR_MAX = Number(process.env.SECTOR_MAX || 10); // per sector
const GENERAL_UI_MAX = Number(process.env.GENERAL_UI_MAX || 18); // UI uses this in index.html; not enforced here
const GENERAL_MAX = Number(process.env.GENERAL_MAX || GENERAL_UI_MAX); // aggregated feed for "GENERAL" view

// Regions / sectors used in UI (index.html)
const REGIONS = ["INDONESIA", "USA", "ASIA", "EUROPE"];
const SECTORS = ["GENERAL", "TECHNOLOGY", "FINANCE", "MINING_ENERGY", "HEALTHCARE", "REGULATION", "CONSUMER"];

const REGION_TZ = {
  INDONESIA: "Asia/Jakarta",
  USA: "America/New_York",
  ASIA: "Asia/Hong_Kong",
  EUROPE: "Europe/London",
};

// Morning brief schedule (local time per region)
const MORNING_RELEASE_HOUR = {
  INDONESIA: 8, // WIB
  USA: 9,       // ET
  ASIA: 7,      // HKT
  EUROPE: 7,    // London
};

// RSS Search presets (Google News RSS Search)
// Notes:
// - baseQ is for GENERAL discovery.
// - anchor is for sector discovery (keeps queries region-relevant without over-constraining to market terms).
const RSS_CFG = {
  INDONESIA: {
    hl: "id", gl: "ID", ceid: "ID:id",
    baseQ: "(Indonesia) (IHSG OR IDX OR Bursa Efek Indonesia OR saham OR rupiah OR Bank Indonesia OR OJK)",
    anchor: "(Indonesia OR IHSG OR IDX OR rupiah OR \"Bank Indonesia\" OR OJK)"
  },
  USA: {
    hl: "en", gl: "US", ceid: "US:en",
    baseQ: "(US OR \"Wall Street\") (stocks OR S&P 500 OR Nasdaq OR Fed OR \"Treasury yields\" OR earnings)",
    anchor: "(US OR \"Wall Street\" OR S&P OR Nasdaq OR Fed OR \"Treasury\")"
  },
  ASIA: {
    hl: "en", gl: "HK", ceid: "HK:en",
    baseQ: "(Asia OR China OR Japan OR Korea OR Hong Kong) (stocks OR markets OR Nikkei OR Hang Seng OR Shanghai OR Kospi)",
    anchor: "(Asia OR China OR Japan OR Korea OR \"Hong Kong\" OR Nikkei OR \"Hang Seng\" OR Shanghai OR Kospi)"
  },
  EUROPE: {
    hl: "en", gl: "GB", ceid: "GB:en",
    baseQ: "(Europe OR EU OR UK) (stocks OR markets OR ECB OR FTSE OR DAX OR CAC OR MIB OR IBEX)",
    anchor: "(Europe OR EU OR UK OR ECB OR FTSE OR DAX OR CAC OR MIB OR IBEX)"
  },
};

const SECTOR_RSS_HINTS = {
  TECHNOLOGY: '(tech OR technology OR teknologi OR AI OR "artificial intelligence" OR semiconductor OR chip OR GPU OR cloud OR software OR cybersecurity OR "data breach")',
  FINANCE: '(bank OR banking OR perbankan OR lender OR "central bank" OR Fed OR ECB OR BoJ OR "interest rate" OR rate OR yield OR bond OR obligasi OR treasury OR stocks OR shares OR equity OR "stock market" OR bursa OR IHSG OR IDX OR "S&P 500" OR Nasdaq OR Wall Street OR rupiah OR "USD/IDR")',
  MINING_ENERGY: '(oil OR crude OR Brent OR WTI OR gas OR LNG OR OPEC OR coal OR batubara OR mining OR tambang OR nickel OR nikel OR copper OR tembaga OR gold OR emas OR silver OR perak OR "energy sector")',
  HEALTHCARE: '(health OR healthcare OR kesehatan OR hospital OR rumahsakit OR pharma OR farmasi OR drug OR obat OR vaccine OR vaksin OR biotech OR BPJS)',
  REGULATION: '(regulation OR regulasi OR regulator OR policy OR kebijakan OR law OR "undang-undang" OR tariff OR tarif OR tax OR pajak OR sanction OR sanksi OR ban OR larangan OR antitrust OR SEC OR DOJ OR FTC OR OJK)',
  CONSUMER: '(consumer OR konsumen OR retail OR ritel OR "consumer goods" OR FMCG OR "e-commerce" OR ecommerce OR travel OR tourism OR airline OR maskapai OR hotel OR restaurant OR makanan OR minuman OR "spending" OR penjualan OR sales)',
};

// Basic allowlist. We rely on RSS source URLs; some are paywalled, but at least it's reputable.
const TRUSTED_DOMAINS = [
  "reuters.com", "cnbc.com", "cnbcindonesia.com", "bloomberg.com", "wsj.com", "ft.com",
  "marketwatch.com", "finance.yahoo.com", "investing.com", "theedgemarkets.com",
  "bisnis.com", "kontan.co.id", "kompas.com", "cnn.com", "nikkei.com", "scmp.com",
  "asia.nikkei.com", "apnews.com", "theguardian.com", "bbc.co.uk", "bbc.com",
  // Indonesia (high-traffic, generally reputable)
  "detik.com", "tempo.co", "katadata.co.id", "antaranews.com", "thejakartapost.com", "jakartaglobe.id",
  "investor.id", "republika.co.id",
  // Global business / markets
  "barrons.com", "forbes.com", "fortune.com", "businessinsider.com", "economist.com",
  "cbsnews.com", "nbcnews.com", "abcnews.go.com", "aljazeera.com",
  // Crypto / tech markets (optional but common)
  "coindesk.com", "cointelegraph.com",
  // Data & calendars
  "tradingeconomics.com", "tradingview.com"
];

// -------- UTIL --------
const storage = new Storage();
const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "", textNodeName: "text" });

const nowIso = () => new Date().toISOString();

function sha1(s) {
  return crypto.createHash("sha1").update(String(s || ""), "utf8").digest("hex").slice(0, 16);
}

function safeDate(x) {
  try { const d = new Date(x); return isNaN(d) ? null : d; } catch { return null; }
}

function hoursAgo(d, now = new Date()) {
  const t = safeDate(d)?.getTime();
  if (!t) return Infinity;
  return (now.getTime() - t) / 36e5;
}

function withinHours(d, h, now = new Date()) {
  return hoursAgo(d, now) <= h;
}

function cleanText(s) {
  return String(s || "").replace(/&nbsp;/g, " ").replace(/\s+/g, " ").trim();
}

function stripHtml(s) {
  return String(s || "").replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function isTrustedUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    if (!["http:", "https:"].includes(u.protocol)) return false;
    if (u.hostname.endsWith("news.google.com")) return true;
    return TRUSTED_DOMAINS.some(d => u.hostname.endsWith(d));
  } catch {
    return false;
  }
}

const TRUSTED_SOURCE_NAME_HINTS = [
  "reuters","bloomberg","cnbc","financial times","ft.com","wall street journal","wsj","marketwatch",
  "the economist","nikkei","associated press","ap news","the verge","techcrunch",
  "kompas","bisnis indonesia","kontan","cnbc indonesia","tempo","antara","katadata",
  "the jakarta post","jakarta post","detik","tirto","kumparan",
  "the guardian","bbc","sky news","al jazeera","yahoo finance","investing.com",
  "barron's","forbes","fortune","semafor","axios"
];

function isTrustedSourceName(name) {
  if (!name) return false;
  const n = String(name).toLowerCase().trim();
  return TRUSTED_SOURCE_NAME_HINTS.some(h => n.includes(h));
}

function isTrustedItem(item) {
  const sources = Array.isArray(item?.sources) ? item.sources : [];
  // Prefer trusting the publisher URL (non-google) if present
  for (const s of sources) {
    const url = s?.url || "";
    try {
      const u = new URL(url);
      if (!u.hostname.endsWith("news.google.com") && isTrustedUrl(url)) return true;
    } catch {}
  }
  // Fall back to source name allowlist (useful when RSS provides weird tracking URLs)
  const primaryName = sources[0]?.name || "";
  if (isTrustedSourceName(primaryName)) return true;

  // If the only URL we have is the Google wrapper, require a trusted source name.
  const primaryUrl = sources[0]?.url || "";
  try {
    const u = new URL(primaryUrl);
    if (u.hostname.endsWith("news.google.com")) return isTrustedSourceName(primaryName);
  } catch {}

  return false;
}


function impactRank(v) {
  const x = String(v || "").toUpperCase();
  if (x === "HIGH") return 3;
  if (x === "MEDIUM") return 2;
  if (x === "LOW") return 1;
  return 0;
}

const SECTOR_KEYWORDS = {
  TECHNOLOGY: [
    "tech","technology","teknologi","ai","artificial intelligence","kecerdasan buatan",
    "semiconductor","semikonduktor","chip","gpu","nvidia","amd","tsmc",
    "software","saas","cloud","aws","azure","gcp","cybersecurity","ransomware","data breach",
    "smartphone","iphone","android","telecom","5g","internet","startup","venture","vc",
    "robot","automation","quantum","openai","gemini","anthropic"
  ],
  FINANCE: [
    "bank","banking","perbankan","lender","loan","kredit","mortgage","hipotek","deposit","savings",
    "central bank","bank sentral","fed","fomc","ecb","boj","bi","bank indonesia","ojk","fdic",
    "interest rate","suku bunga","rate cut","rate hike","yield","yields","bond","bonds","obligasi","treasury","t-bill",
    "inflation","cpi","ppi","unemployment","nfp","gdp","growth","resesi","recession",
    "fx","forex","currency","dollar","usd","eur","jpy","rupiah","idr","usd/idr","dxy",
    "stocks","stock","shares","equity","equities","index","indices","bursa","ihsg","idx","nasdaq","s&p","dow","wall street",
    "trading","sell-off","rally","market","markets","risk-on","risk-off","capital flow","outflow","inflow",
    "earnings","profit","laba","guidance","dividend","buyback","ipo"
  ],
  MINING_ENERGY: [
    "oil","crude","brent","wti","opec","gas","lng","natural gas","diesel","fuel","bbm",
    "energy","energi","power","electricity","listrik","utility","utilities",
    "coal","batubara","mining","tambang","minerals","mineral","smelter","refinery",
    "nickel","nikel","copper","tembaga","tin","timah","bauxite","bauksit",
    "gold","emas","silver","perak","platinum","palladium",
    "ev","electric vehicle","baterai","battery","lithium","rare earth"
  ],
  HEALTHCARE: [
    "health","healthcare","kesehatan","hospital","rumahsakit","clinic","klinik",
    "pharma","farmasi","drug","obat","vaccine","vaksin","biotech","biotek",
    "medical","medis","doctor","dokter","nurse","perawat","bpjs","insurance health","asuransi kesehatan"
  ],
  REGULATION: [
    "regulation","regulasi","regulator","policy","kebijakan","law","undang-undang","peraturan",
    "sanction","sanksi","ban","larangan","tariff","tarif","tax","pajak","subsidy","subsidi",
    "antitrust","competition","monopoly","probe","investigation","penyelidikan",
    "sec","doj","ftc","eu commission","ojk","bappebti","kppu","kemenkeu","kementerian keuangan"
  ],
  CONSUMER: [
    "consumer","konsumen","retail","ritel","fmcg","consumer goods","e-commerce","ecommerce","marketplace",
    "shopping","belanja","sales","penjualan","spending","pengeluaran",
    "travel","tourism","pariwisata","airline","maskapai","hotel","restaurant","restoran",
    "food","makanan","beverage","minuman","grocery","supermarket",
    "automotive","otomotif","car","mobil","motorcycle","motor","ride-hailing","gojek","grab"
  ],
};

const IMPACT_HIGH = ["plunge", "surge", "crash", "default", "recession", "inflation", "rate", "tariff", "sanction", "war", "ban", "cuts", "hikes", "emergency", "shock"];
const IMPACT_MED  = ["earnings", "beats", "misses", "merger", "acquisition", "ipo", "lawsuit", "deal", "guidance", "probe", "investigation"];

function normText(s) {
  return String(s || "").toLowerCase().replace(/[^a-z0-9\s\/\-&]/g, " ").replace(/\s+/g, " ").trim();
}

function escapeRegExp(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasToken(t, token) {
  const tok = String(token || "").trim().toLowerCase();
  if (!tok) return false;
  // For multi-word phrases, substring match is fine.
  if (tok.includes(" ")) return t.includes(tok);
  // Word-boundary match for short tokens to avoid false positives.
  try {
    return new RegExp(`\\b${escapeRegExp(tok)}\\b`, "i").test(t);
  } catch {
    return t.includes(tok);
  }
}

function inferSectors(headline, description) {
  const text = normText([headline, description].filter(Boolean).join(" "));
  if (!text) return ["GENERAL"];

  const scoreFor = (keywords) => {
    let score = 0;
    for (const kw of keywords) {
      if (!kw) continue;
      if (hasToken(text, kw)) {
        // phrases are stronger than single tokens
        score += (kw.includes(" ") || kw.includes("/") || kw.includes("-")) ? 2 : 1;
      }
    }
    return score;
  };

  const scored = Object.entries(SECTOR_KEYWORDS)
    .map(([sector, kws]) => ({ sector, score: scoreFor(kws) }))
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return ["GENERAL"];

  // pick top sectors, but keep it sane
  const top = scored.slice(0, 3).map(x => x.sector);

  // If tech and finance both triggered by generic words, prefer TECH only when tech score meaningfully high
  // (prevents 'market' from hijacking everything into FINANCE)
  if (top.includes("TECHNOLOGY") && top.includes("FINANCE")) {
    const sTech = scored.find(x => x.sector === "TECHNOLOGY")?.score || 0;
    const sFin = scored.find(x => x.sector === "FINANCE")?.score || 0;
    if (sFin >= sTech + 4) {
      // finance dominates
      return ["FINANCE", ...top.filter(x => x !== "FINANCE")].slice(0, 3);
    }
    if (sTech >= sFin + 4) {
      return ["TECHNOLOGY", ...top.filter(x => x !== "TECHNOLOGY")].slice(0, 3);
    }
    // otherwise keep both
  }

  return top;
}


function inferSector(text) {
  return inferSectors(text)[0] || "GENERAL";
}

function inferImpact(text) {
  const t = String(text || "").toLowerCase();
  if (IMPACT_HIGH.some(k => t.includes(k))) return "HIGH";
  if (IMPACT_MED.some(k => t.includes(k))) return "MEDIUM";
  return "LOW";
}

function fallbackKeypoints(description) {
  const s = stripHtml(description || "");
  if (!s) return [];
  const parts = s.split(/(?<=[\.\!\?])\s+|;\s+/).map(cleanText).filter(Boolean);
  const out = [];
  for (const p of parts) {
    if (out.length >= 3) break;
    if (p.length < 18) continue;
    out.push(p);
  }
  return out.slice(0, 3);
}

function canonicalUrl(u) {
  return String(u || "").trim().replace(/&amp;/g, "&");
}

function makeBaseItem({ region, headline, link, publishedAt, description, sourceName, sourceUrl, hintSector }) {
  const url = canonicalUrl(sourceUrl || link);
  const storyKey = sha1(`${url}|${headline}`);
  const snippet = cleanText(stripHtml(description || ""));
  // Keep a lightweight fallback story so UI is never empty just because LLM is down.
  const fallbackStory = snippet ? snippet.slice(0, 650) : "";
  const hinted = (hintSector && SECTORS.includes(String(hintSector).toUpperCase())) ? String(hintSector).toUpperCase() : null;
  let sectors = inferSectors(`${headline} ${description || ""}`);
  if (hinted && hinted !== "GENERAL") sectors = Array.from(new Set([hinted, ...sectors]));
  return {
    id: `${storyKey}:${region}`,
    storyKey,
    region,
    sector: (hinted && hinted !== "GENERAL") ? hinted : (sectors && sectors.length ? sectors[0] : "GENERAL"),
    sectors: (sectors && sectors.length ? sectors : undefined),
    impact: inferImpact(`${headline} ${description || ""}`),
    headline: cleanText(headline),
    keypoints: fallbackKeypoints(description),
    story: fallbackStory,
    publishedAt,
    sources: [{ name: cleanText(sourceName || "Source"), url }],
  };
}

function enforceQuota(items, maxTotal = SECTOR_MAX) {
  const want = { HIGH: 4, MEDIUM: 4, LOW: 2 };
  const by = { HIGH: [], MEDIUM: [], LOW: [] };

  for (const it of items) {
    const k = String(it.impact || "MEDIUM").toUpperCase();
    (by[k] || by.MEDIUM).push(it);
  }

  for (const k of Object.keys(by)) {
    by[k].sort((a, b) => (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0));
  }

  const out = [];
  for (const k of ["HIGH", "MEDIUM", "LOW"]) out.push(...by[k].slice(0, want[k]));

  if (out.length < maxTotal) {
    const used = new Set(out.map(x => x.storyKey));
    const rest = items.filter(x => !used.has(x.storyKey)).sort((a, b) => {
      const ia = impactRank(a.impact), ib = impactRank(b.impact);
      if (ia !== ib) return ib - ia;
      return (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0);
    });
    for (const it of rest) {
      if (out.length >= maxTotal) break;
      out.push(it);
    }
  }

  return out.slice(0, maxTotal);
}

// -------- RSS --------
async function fetchRss(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (compatible; VelraTerminal/1.0)",
      "accept": "application/rss+xml,application/xml,text/xml,*/*",
    },
  });
  if (!res.ok) throw new Error(`RSS HTTP ${res.status}`);
  const txt = await res.text();
  const obj = parser.parse(txt);
  const items = obj?.rss?.channel?.item;
  if (!items) return [];
  const arr = Array.isArray(items) ? items : [items];
  return arr.map(it => ({
    title: cleanText(it.title?.text ?? it.title),
    link: canonicalUrl(it.link?.text ?? it.link),
    pubDate: it.pubDate?.text ?? it.pubDate,
    description: it.description?.text ?? it.description,
    sourceName: cleanText(it.source?.text ?? it.source),
    sourceUrl: canonicalUrl(it.source?.url ?? ""),
  })).filter(x => x.title && x.link);
}

function buildRssUrl(region, q) {
  const cfg = RSS_CFG[region];
  // NOTE: RSS is for discovery, so it should cover the whole TTL window.
  // LOOKBACK_HOURS is only for deciding which stories are "new" and should be rewritten by Gemini.
  const h = Math.max(1, Math.floor(RSS_WINDOW_HOURS));
  const when = h >= 24 ? "1d" : `${h}h`;
  const query = `${q} when:${when}`;
  return `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=${cfg.hl}&gl=${cfg.gl}&ceid=${cfg.ceid}`;
}

async function gatherRegionNews(region) {
  const cfg = RSS_CFG[region] || { baseQ: region, anchor: region };
  const base = cfg.baseQ || region;
  const anchor = cfg.anchor || region;

  // Discovery strategy:
  // - GENERAL feed: broad market query (baseQ)
  // - Sector feeds: (anchor) AND (sector hints) to force sector coverage.
  // This avoids the classic failure where everything collapses into GENERAL.
  const queries = [
    { q: base, hintSector: "GENERAL" },
    ...Object.entries(SECTOR_RSS_HINTS).map(([sector, q]) => ({
      q: `(${anchor}) ${q}`,
      hintSector: sector
    })),
  ];

  const jobs = queries.map(({ q, hintSector }) => ({
    hintSector,
    url: buildRssUrl(region, q)
  }));

  const results = await Promise.allSettled(jobs.map(j =>
    fetchRss(j.url).then(items => items.map(it => ({ ...it, hintSector: j.hintSector })))
  ));

  const rows = [];
  for (const r of results) if (r.status === "fulfilled") rows.push(...r.value);

  // Deduplicate by URL.
  // If the same article shows up in multiple feeds, choose the best sector hint:
  // - Prefer specific hint over GENERAL
  // - Prefer hint that matches keyword inference on the headline/description
  const seen = new Map(); // key -> item
  for (const x of rows) {
    const u = x.sourceUrl || x.link;
    const key = sha1(u);
    const prev = seen.get(key);
    if (!prev) { seen.set(key, x); continue; }

    const prevHint = String(prev.hintSector || "GENERAL").toUpperCase();
    const newHint = String(x.hintSector || "GENERAL").toUpperCase();

    const textPrev = `${prev.title || ""} ${prev.description || ""}`;
    const textNew = `${x.title || ""} ${x.description || ""}`;
    const infPrev = inferSectors(textPrev);
    const infNew = inferSectors(textNew);

    function score(hint, inf) {
      const h = String(hint || "GENERAL").toUpperCase();
      let s = 0;
      if (h !== "GENERAL") s += 2;
      if (inf && inf.length) {
        if (inf[0] === h) s += 3;
        else if (inf.includes(h)) s += 2;
      }
      return s;
    }

    const sPrev = score(prevHint, infPrev);
    const sNew = score(newHint, infNew);

    if (sNew > sPrev) {
      seen.set(key, x);
    } else if (sNew === sPrev) {
      // tie-breaker: pick the newer pubDate
      const tPrev = safeDate(prev.pubDate)?.getTime() || 0;
      const tNew = safeDate(x.pubDate)?.getTime() || 0;
      if (tNew > tPrev) seen.set(key, x);
    }
  }

  return Array.from(seen.values());
}


// -------- MACRO (Economic Calendar, free) --------
// Source: Forex Factory weekly calendar XML (public). Contains previous/forecast/actual.
const MACRO_CAL_URL = process.env.MACRO_CAL_URL || "https://nfs.faireconomy.media/ff_calendar_thisweek.xml";
const MACRO_CACHE_HOURS = Number(process.env.MACRO_CACHE_HOURS || 6);

// Map region -> relevant currencies (ForexFactory codes)
const REGION_CCY = {
  USA: ["USD"],
  EUROPE: ["EUR", "GBP"],
  ASIA: ["JPY", "CNY", "HKD", "KRW"],
  // IDR is often not available; show global drivers that typically move EM risk.
  INDONESIA: ["USD", "CNY", "JPY"],
};

const MACRO_KEYWORDS = [
  "Non-Farm", "NFP", "Unemployment", "CPI", "PPI", "GDP", "Retail Sales",
  "PMI", "Manufacturing", "Services", "Fed", "FOMC", "Rate", "Interest",
  "Core", "Inflation", "Payroll", "Jobless", "Confidence", "Trade Balance"
];

function nodeText(n) {
  if (n == null) return "";
  if (typeof n === "string") return cleanText(n);
  if (typeof n === "object" && "text" in n) return cleanText(n.text);
  return cleanText(String(n));
}

function normalizeMacroVal(v) {
  const s = cleanText(String(v || ""));
  if (!s || s === " " || s.toLowerCase() === "na") return "";
  return s;
}

function macroInteresting(title) {
  const t = String(title || "");
  return MACRO_KEYWORDS.some(k => t.toLowerCase().includes(String(k).toLowerCase()));
}

async function fetchMacroCalendar() {
  const res = await fetch(MACRO_CAL_URL, {
    headers: {
      "user-agent": "Mozilla/5.0 (compatible; VelraTerminal/1.0)",
      "accept": "application/xml,text/xml,*/*",
    },
  });
  if (!res.ok) throw new Error(`macro cal HTTP ${res.status}`);
  const xml = await res.text();
  const obj = parser.parse(xml);
  const ev = obj?.weeklyevents?.event || obj?.weeklyevents?.events?.event || obj?.weeklyevents?.events || obj?.event;
  if (!ev) return [];
  const arr = Array.isArray(ev) ? ev : [ev];
  return arr.map(e => ({
    title: nodeText(e.title),
    country: nodeText(e.country),
    currency: nodeText(e.currency),
    date: nodeText(e.date),
    time: nodeText(e.time),
    impact: nodeText(e.impact),
    actual: normalizeMacroVal(nodeText(e.actual)),
    forecast: normalizeMacroVal(nodeText(e.forecast)),
    previous: normalizeMacroVal(nodeText(e.previous)),
    url: nodeText(e.url),
  })).filter(x => x.title && x.currency);
}

function sortMacro(a, b) {
  // Sort by date/time string fallback.
  const ka = `${a.date} ${a.time}`.trim();
  const kb = `${b.date} ${b.time}`.trim();
  return ka.localeCompare(kb);
}

async function buildMacro(prevMacro = null, now = new Date()) {
  try {
    const prevAt = safeDate(prevMacro?.updatedAt);
    if (prevAt && withinHours(prevAt.toISOString(), MACRO_CACHE_HOURS, now)) return prevMacro;
  } catch { /* ignore */ }

  let events = [];
  try {
    events = await fetchMacroCalendar();
  } catch (e) {
    console.warn("[worker] macro fetch failed:", e?.message || e);
    return prevMacro || { updatedAt: nowIso(), source: "forex_factory", regions: {} };
  }

  // Filter to the stuff people actually care about.
  events = events.filter(e => macroInteresting(e.title));

  const out = { updatedAt: nowIso(), source: "forex_factory", regions: {} };
  for (const region of REGIONS) {
    const ccys = REGION_CCY[region] || [];
    const rEv = events.filter(e => ccys.includes(e.currency));
    const recent = rEv.filter(e => e.actual).sort(sortMacro).slice(-10).reverse(); // last released
    const next = rEv.filter(e => !e.actual).sort(sortMacro).slice(0, 10);

    // "metric" tab = recent releases; "next" tab = upcoming; "trend" tab = same as recent for now.
    out.regions[region] = {
      metric: recent.map(e => ({
        metric: e.title,
        previous: e.previous,
        forecast: e.forecast,
        actual: e.actual,
        currency: e.currency,
        when: `${e.date} ${e.time}`.trim(),
        impact: e.impact || "",
        url: e.url || "",
        source: "ForexFactory",
      })),
      trend: recent.map(e => ({
        metric: e.title,
        previous: e.previous,
        forecast: e.forecast,
        actual: e.actual,
        currency: e.currency,
        when: `${e.date} ${e.time}`.trim(),
        impact: e.impact || "",
        url: e.url || "",
        source: "ForexFactory",
      })),
      next: next.map(e => ({
        metric: e.title,
        previous: e.previous,
        forecast: e.forecast,
        actual: e.actual,
        currency: e.currency,
        when: `${e.date} ${e.time}`.trim(),
        impact: e.impact || "",
        url: e.url || "",
        source: "ForexFactory",
      })),
    };
  }

  return out;
}


// -------- INDICATORS --------
const INDICATOR_SYMBOLS = {
  INDONESIA: [
    { code: "IHSG", name: "IDX Composite", symbol: "^JKSE" },
    { code: "IDX30", name: "IDX30", symbol: "^IDX30" },
    { code: "LQ45", name: "LQ45", symbol: "^JKLQ45" },
    { code: "USDIDR", name: "USD/IDR", symbol: "IDR=X" },
    { code: "GOLD", name: "Gold", symbol: "GC=F" },
    { code: "SILVER", name: "Silver", symbol: "SI=F" },
    { code: "OIL", name: "WTI Crude", symbol: "CL=F" },
  ],
  USA: [
    { code: "SPX", name: "S&P 500", symbol: "^GSPC" },
    { code: "IXIC", name: "Nasdaq Composite", symbol: "^IXIC" },
    { code: "VIX", name: "VIX", symbol: "^VIX" },
    { code: "DXY", name: "US Dollar Index", symbol: "DX-Y.NYB" },
    { code: "USDCNY", name: "USD/CNY", symbol: "CNY=X" },
    { code: "GOLD", name: "Gold", symbol: "GC=F" },
    { code: "SILVER", name: "Silver", symbol: "SI=F" },
    { code: "OIL", name: "WTI Crude", symbol: "CL=F" },
    { code: "BTC", name: "Bitcoin", symbol: "BTC-USD" },
  ],
  ASIA: [
    { code: "N225", name: "Nikkei 225", symbol: "^N225" },
    { code: "SSE", name: "Shanghai Composite", symbol: "000001.SS" },
    { code: "HSI", name: "Hang Seng", symbol: "^HSI" },
    { code: "KS11", name: "KOSPI", symbol: "^KS11" },
    { code: "GOLD", name: "Gold", symbol: "GC=F" },
    { code: "OIL", name: "WTI Crude", symbol: "CL=F" },
  ],
  EUROPE: [
    { code: "FTSE", name: "FTSE 100", symbol: "^FTSE" },
    { code: "DAX", name: "DAX", symbol: "^GDAXI" },
    { code: "CAC", name: "CAC 40", symbol: "^FCHI" },
    { code: "MIB", name: "FTSE MIB", symbol: "FTSEMIB.MI" },
    { code: "IBEX", name: "IBEX 35", symbol: "^IBEX" },
    { code: "GOLD", name: "Gold", symbol: "GC=F" },
    { code: "OIL", name: "WTI Crude", symbol: "CL=F" },
  ],
};

async function fetchYahooChart(symbol) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=5d`;
  const res = await fetch(url, { headers: { "user-agent": "Mozilla/5.0" } });
  if (!res.ok) throw new Error(`yahoo ${symbol} ${res.status}`);
  const j = await res.json();
  const r = j?.chart?.result?.[0];
  const closes = r?.indicators?.quote?.[0]?.close || [];
  const ts = r?.timestamp || [];
  const nums = closes.filter(x => typeof x === "number");
  const last = nums.slice(-1)[0];
  const prev = nums.slice(-2)[0];
  const tLast = ts.slice(-1)[0] ? new Date(ts.slice(-1)[0] * 1000).toISOString() : null;
  if (typeof last !== "number") throw new Error("no price");
  const changePct = (typeof prev === "number" && prev !== 0) ? ((last - prev) / prev) * 100 : null;
  return { value: last, changePct, updatedAt: tLast };
}

function fmtNum(x) {
  if (typeof x !== "number" || !isFinite(x)) return "";
  if (Math.abs(x) >= 1000) return x.toLocaleString("en-US", { maximumFractionDigits: 2 });
  return x.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

async function buildIndicators(prevIndicators = null) {
  const out = {};
  for (const region of REGIONS) out[region] = [];

  const tasks = [];
  for (const region of REGIONS) {
    for (const s of (INDICATOR_SYMBOLS[region] || [])) {
      tasks.push((async () => {
        try {
          const r = await fetchYahooChart(s.symbol);
          out[region].push({
            code: s.code,
            name: s.name,
            symbol: s.symbol,
            value: r.value,
            valueFmt: fmtNum(r.value),
            changePct: (typeof r.changePct === "number") ? r.changePct : null,
            updatedAt: r.updatedAt,
          });
        } catch {
          // silent
        }
      })());
    }
  }
  await Promise.all(tasks);

  if (prevIndicators) {
    for (const region of REGIONS) {
      if (!out[region].length && Array.isArray(prevIndicators?.[region]) && prevIndicators[region].length) {
        out[region] = prevIndicators[region];
      }
    }
  }

  for (const region of REGIONS) {
    const order = (INDICATOR_SYMBOLS[region] || []).map(x => x.code);
    out[region].sort((a, b) => order.indexOf(a.code) - order.indexOf(b.code));
  }

  return out;
}

// -------- GEMINI WRITING --------
const ai = GEMINI_API_KEY ? new GoogleGenAI({ apiKey: GEMINI_API_KEY }) : null;

function parseJsonFromText(text) {
  if (!text) return null;
  try {
    const clean = String(text).replace(/```json|```/g, "").trim();
    const startObj = clean.indexOf("{");
    const startArr = clean.indexOf("[");
    if (startArr !== -1 && (startObj === -1 || startArr < startObj)) {
      return JSON.parse(clean.substring(startArr, clean.lastIndexOf("]") + 1));
    }
    if (startObj !== -1) return JSON.parse(clean.substring(startObj, clean.lastIndexOf("}") + 1));
    return null;
  } catch {
    return null;
  }
}

function sanitizeRewritten(base, rewritten) {
  const out = { ...base };
  if (rewritten && typeof rewritten === "object") {
    const headline = cleanText(rewritten.headline);
    const impact = String(rewritten.impact || out.impact).toUpperCase();
    const sector = String(rewritten.sector || out.sector).toUpperCase();

    const kp = Array.isArray(rewritten.keypoints)
      ? rewritten.keypoints.map(x => cleanText(x)).filter(Boolean).slice(0, 3)
      : null;

    const story = cleanText(rewritten.story || "");

    out.headline = headline || out.headline;
    out.impact = ["HIGH", "MEDIUM", "LOW"].includes(impact) ? impact : out.impact;
    out.sector = SECTORS.includes(sector) ? sector : out.sector;

    // Keep sectors[] in sync (used for sector views)
    if (out.sector && out.sector !== "GENERAL") {
      const prevS = Array.isArray(out.sectors) ? out.sectors : [];
      out.sectors = Array.from(new Set([out.sector, ...prevS])).filter(s => s && s !== "GENERAL");
    }

    // Preserve existing keypoints/story if Gemini returns empty fields.
    if (kp && kp.length) out.keypoints = kp;

    if (story && story.length >= 40) {
      out.story = story;
    } else if (!out.story || out.story.length < 40) {
      // last resort: build a minimal story from keypoints
      const kps = (out.keypoints || []).filter(Boolean);
      if (kps.length) out.story = kps.join(" ");
    }

    out.llm = true;
  }
  return out;
}

async function rewriteItemsWithGemini(region, items) {
  if (!ai) return items;
  if (!items.length) return items;

  const modelFallback = [
    GEMINI_MODEL,
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
  ].filter(Boolean);

  const payload = items.map(it => ({
    storyKey: it.storyKey,
    headline: it.headline,
    description: it.keypoints?.join(" ") || "",
    publishedAt: it.publishedAt,
    sourceName: it.sources?.[0]?.name || "Source",
    sourceUrl: it.sources?.[0]?.url || "",
    suggestedSector: it.sector,
    suggestedImpact: it.impact,
  }));

  const prompt = `
Kamu adalah editor pasar finansial. Kamu TIDAK mencari berita; kamu hanya menulis ulang berdasarkan data input.
Aturan keras:
- Jangan menambah fakta baru di luar input headline/description.
- Jangan mengarang angka, kutipan, atau detail yang tidak ada.
- Bahasa Indonesia profesional, singkat, terminal-style.
- Output valid JSON (tanpa markdown).

INPUT (array):
${JSON.stringify(payload, null, 2)}

TUGAS:
Untuk setiap item, tulis ulang menjadi objek:
{
  "storyKey": "...",
  "headline": "headline yang lebih rapi (maks 110 char)",
  "sector": one of ${JSON.stringify(SECTORS)},
  "impact": "HIGH|MEDIUM|LOW",
  "keypoints": ["3 poin ringkas"],
  "story": "narasi 90-140 kata, ringkas, tanpa filler"
}

OUTPUT: JSON array (urutan bebas).
`.trim();

  for (const model of modelFallback) {
    try {
      const res = await ai.models.generateContent({
        model,
        contents: prompt,
        config: { temperature: GEMINI_TEMPERATURE, maxOutputTokens: GEMINI_MAX_TOKENS },
      });
      const parsed = parseJsonFromText(res.text);
      if (!Array.isArray(parsed)) throw new Error("Gemini output not array");

      const map = new Map();
      for (const o of parsed) if (o?.storyKey) map.set(String(o.storyKey), o);
      return items.map(it => sanitizeRewritten(it, map.get(it.storyKey)));
    } catch (e) {
      console.warn(`[worker] Gemini rewrite failed model=${model} region=${region}:`, e?.message || e);
    }
  }
  return items;
}

async function genMorningBriefText(region, topItems) {
  if (!ai) return null;
  const tz = REGION_TZ[region] || "UTC";
  const asOf = new Intl.DateTimeFormat("id-ID", { timeZone: tz, dateStyle: "medium", timeStyle: "short" }).format(new Date());

  const input = topItems.slice(0, 10).map(it => ({
    headline: it.headline,
    sector: it.sector,
    impact: it.impact,
    source: it.sources?.[0]?.name || ""
  }));

  const prompt = `
Kamu menulis Morning Brief pasar untuk region ${region}.
Aturan:
- Jangan mengarang fakta baru.
- Berdasarkan list headline berikut.
- Output JSON tanpa markdown.

HEADLINE LIST:
${JSON.stringify(input, null, 2)}

OUTPUT JSON:
{
  "title": "Judul singkat",
  "lede": "Paragraf pembuka 2-3 kalimat (terminal style)",
  "playbook": ["3-5 poin watchlist / playbook hari ini (bukan nasihat keuangan)"]
}
`.trim();

  const modelFallback = [
    GEMINI_MODEL,
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
  ].filter(Boolean);

  for (const model of modelFallback) {
    try {
      const res = await ai.models.generateContent({
        model,
        contents: prompt,
        config: { temperature: 0.35, maxOutputTokens: 900 }
      });
      const parsed = parseJsonFromText(res.text);
      if (!parsed || typeof parsed !== "object") throw new Error("MorningBrief output invalid");
      return {
        asOf,
        title: cleanText(parsed.title || ""),
        lede: cleanText(parsed.lede || ""),
        playbook: Array.isArray(parsed.playbook) ? parsed.playbook.map(x => cleanText(x)).filter(Boolean).slice(0, 6) : [],
      };
    } catch (e) {
      console.warn(`[worker] Gemini morningBrief failed model=${model} region=${region}:`, e?.message || e);
    }
  }
  return null;
}

// -------- SNAPSHOT IO --------
async function readSnapshot() {
  if (BUCKET) {
    try {
      const file = storage.bucket(BUCKET).file(OBJECT);
      const [exists] = await file.exists();
      if (!exists) return null;
      const [buf] = await file.download();
      return JSON.parse(buf.toString("utf-8"));
    } catch (e) {
      console.warn("[worker] snapshot read from GCS failed:", e?.message || e);
    }
  }
  try {
    const p = path.join(process.cwd(), OBJECT);
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, "utf-8"));
  } catch {
    return null;
  }
}

async function writeSnapshot(data) {
  const txt = JSON.stringify(data, null, 2);
  if (BUCKET) {
    const file = storage.bucket(BUCKET).file(OBJECT);
    await file.save(txt, {
      contentType: "application/json",
      resumable: false,
      validation: false,
      metadata: { cacheControl: "public, max-age=0, no-cache, no-store, must-revalidate" }
    });
    console.log(`[worker] wrote gs://${BUCKET}/${OBJECT} bytes=${txt.length}`);
    return;
  }
  fs.writeFileSync(path.join(process.cwd(), OBJECT), txt);
  console.log(`[worker] wrote local ${OBJECT} bytes=${txt.length}`);
}

function trimLivewire(items, now = new Date()) {
  return (items || []).filter(x => x && withinHours(x.publishedAt, TTL_HOURS, now));
}

function mergeByStoryKey(oldItems, newItems) {
  const map = new Map();
  for (const it of (oldItems || [])) map.set(it.storyKey, it);
  for (const it of (newItems || [])) {
    const prev = map.get(it.storyKey);
    if (!prev) { map.set(it.storyKey, it); continue; }
    const prevT = safeDate(prev.publishedAt)?.getTime() || 0;
    const newT = safeDate(it.publishedAt)?.getTime() || 0;
    if (newT >= prevT) {
      map.set(it.storyKey, {
        ...prev,
        ...it,
        sector: (it.sector && it.sector !== "GENERAL") ? it.sector : (prev.sector || it.sector || "GENERAL"),
        sectors: (Array.isArray(it.sectors) && it.sectors.length) ? it.sectors : (Array.isArray(prev.sectors) ? prev.sectors : undefined),
        story: it.story || prev.story || "",
        keypoints: (it.keypoints && it.keypoints.length) ? it.keypoints : (prev.keypoints || []),
      });
    }
  }
  return Array.from(map.values());
}


function livewireKey(it) {
  // Keep storyKey stable for LLM caching, but snapshot must be unique per region+sector view.
  return it?.id || `${it.storyKey}:${it.region}:${it.sector}`;
}

function collapseByStoryKey(items) {
  const map = new Map();
  const score = (x) => {
    const llm = x?.llm ? 1000 : 0;
    const story = x?.story ? Math.min(500, String(x.story).length) : 0;
    const kp = Array.isArray(x?.keypoints) ? x.keypoints.length * 10 : 0;
    const impact = impactRank(x?.impact) * 5;
    const sectorBonus = (x?.sector && x.sector !== "GENERAL") ? 20 : 0;
    return llm + story + kp + impact + sectorBonus;
  };
  for (const it of (items || [])) {
    if (!it || !it.storyKey) continue;
    const prev = map.get(it.storyKey);
    if (!prev) { map.set(it.storyKey, it); continue; }
    if (score(it) >= score(prev)) map.set(it.storyKey, it);
  }
  return Array.from(map.values());
}

function mergeLivewireByKey(oldItems, newItems) {
  const map = new Map();
  for (const it of (oldItems || [])) map.set(livewireKey(it), it);
  for (const it of (newItems || [])) {
    const k = livewireKey(it);
    const prev = map.get(k);
    if (!prev) { map.set(k, it); continue; }
    const prevT = safeDate(prev.publishedAt)?.getTime() || 0;
    const newT = safeDate(it.publishedAt)?.getTime() || 0;
    if (newT >= prevT) {
      map.set(k, {
        ...prev,
        ...it,
        // Preserve richest content available
        story: (it.story && it.story.length >= 40) ? it.story : (prev.story || ""),
        keypoints: (it.keypoints && it.keypoints.length) ? it.keypoints : (prev.keypoints || []),
      });
    }
  }
  return Array.from(map.values());
}

function localDateKey(region, date = new Date()) {
  const tz = REGION_TZ[region] || "UTC";
  return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(date);
}

function localHourMinute(region, date = new Date()) {
  const tz = REGION_TZ[region] || "UTC";
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: tz, hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(date);
  const get = (t) => parts.find(p => p.type === t)?.value;
  return { hour: Number(get("hour")), minute: Number(get("minute")) };
}

function extractWatchlist(region, items) {
  const texts = items.map(x => x.headline).join(" ");
  const tickers = new Set();
  if (region === "INDONESIA") {
    (texts.match(/\b[A-Z]{4}\b/g) || []).forEach(t => tickers.add(t));
  } else if (region === "USA") {
    const bad = new Set(["US","AI","THE","AND","FED","ECB","OJK","BI","EU","UK","DJI","SPX","ET","HKT"]);
    (texts.match(/\b[A-Z]{1,5}\b/g) || []).forEach(t => {
      if (t.length === 1) return;
      if (bad.has(t)) return;
      tickers.add(t);
    });
  }
  return Array.from(tickers).slice(0, 10);
}

function digestBullets(items) {
  const seen = new Set();
  const out = [];
  const sorted = items.slice().sort((a, b) => {
    const ia = impactRank(a.impact), ib = impactRank(b.impact);
    if (ia !== ib) return ib - ia;
    return (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0);
  });

  for (const it of sorted) {
    const kps = Array.isArray(it.keypoints) ? it.keypoints : [];
    for (const kp of kps) {
      const t = cleanText(kp);
      if (!t || t.length < 18) continue;
      const key = t.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(t);
      if (out.length >= 6) return out;
    }
  }

  // fallback: headlines if keypoints are scarce
  for (const it of sorted) {
    const t = cleanText(it.headline);
    if (!t) continue;
    const key = t.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(t);
    if (out.length >= 6) break;
  }

  return out;
}

function buildDigest(region, sector, items, generatedAtIso) {
  const bullets = digestBullets(items).slice(0, 6);
  const wl = extractWatchlist(region, items).map(t => `${t} — pantau reaksi headline & level kunci.`);
  return {
    title: sector === "GENERAL" ? "Market Update" : `${sector.replace("_", " ")} Update`,
    bullets,
    watchlist: wl,
    updatedAt: generatedAtIso,
  };
}

function pickTop(items, n = 3) {
  return items.slice().sort((a, b) => {
    const ia = impactRank(a.impact), ib = impactRank(b.impact);
    if (ia !== ib) return ib - ia;
    return (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0);
  }).slice(0, n);
}

function buildMorningDeck(region, topItems, indicatorsForRegion, mbText) {
  const tz = REGION_TZ[region] || "UTC";
  const asOf = mbText?.asOf || new Intl.DateTimeFormat("id-ID", { timeZone: tz, dateStyle: "medium", timeStyle: "short" }).format(new Date());
  const title = mbText?.title || `${region === "INDONESIA" ? "Indonesia" : region === "USA" ? "USA" : region} Morning Brief`;
  const lede = mbText?.lede || (topItems.length ? `Tema utama: ${topItems.slice(0, 3).map(x => x.headline).join(" · ")}.` : "");

  const slides = [];

  if (Array.isArray(indicatorsForRegion) && indicatorsForRegion.length) {
    slides.push({
      type: "market_snapshot",
      title: "Market Snapshot",
      items: indicatorsForRegion.slice(0, 6).map(x => ({
        label: x.code,
        value: x.value,
        valueFmt: x.valueFmt,
        changePct: x.changePct,
        note: x.name || ""
      })),
    });
  }

  const impactCards = pickTop(topItems, 3);
  if (impactCards.length) {
    slides.push({
      type: "impact_cards",
      title: "Top Drivers",
      cards: impactCards.map(it => ({
        title: it.headline.slice(0, 90),
        body: (it.keypoints || []).slice(0, 2).join(" ")
      }))
    });
  }

  if (topItems.length) {
    slides.push({
      type: "news_list",
      title: "Headlines (≤24 jam)",
      items: topItems.slice(0, 8).map(it => ({
        category: it.sector === "REGULATION" ? "REGULASI / PEMERINTAH" : "KORPORASI / SEKTOR",
        headline: it.headline,
        keypoints: (it.keypoints || []).slice(0, 3),
        publishedAt: it.publishedAt,
        sources: (it.sources || []).slice(0, 1),
      }))
    });
  }

  const deep = pickTop(topItems.filter(x => x.story && x.story.length >= 40), 1)[0];
  if (deep) {
    slides.push({
      type: "deep_dive",
      title: "Deep Dive",
      item: {
        headline: deep.headline,
        body: deep.story,
        bullets: (deep.keypoints || []).slice(0, 4),
        sources: (deep.sources || []).slice(0, 1),
      }
    });
  }

  const watchlist = extractWatchlist(region, topItems).slice(0, 8).map(t => ({ ticker: t, why: "Pantau reaksi headline & level kunci." }));
  const playbook = (mbText?.playbook && mbText.playbook.length)
    ? mbText.playbook
    : [
      "Pantau headline kebijakan dan data makro yang dapat mengubah risk sentiment.",
      "Fokus pada berita berdampak tinggi; noise harian abaikan.",
      "Gunakan sizing yang wajar saat volatilitas naik.",
      "Bukan nasihat keuangan."
    ];

  slides.push({
    type: "watchlist_playbook",
    title: "Watchlist & Playbook",
    watchlist,
    playbook,
    disclaimer: "Bukan nasihat keuangan."
  });

  return { title, asOf, lede, slides };
}

// -------- MAIN --------
async function main() {
  const startedAt = new Date();
  console.log(`[worker] RUN start now=${startedAt.toISOString()} mode=${RUN_MODE} lookback=${LOOKBACK_HOURS}h ttl=${TTL_HOURS}h`);

  const debug = { rssWindowHours: RSS_WINDOW_HOURS, lookbackHours: LOOKBACK_HOURS, ttlHours: TTL_HOURS, regions: {} };

  const prev = await readSnapshot();
  const prevLivewire = trimLivewire(prev?.livewire || [], startedAt);
  const prevIndicators = prev?.indicators || prev?.indices || null;

  const indicators = await buildIndicators(prevIndicators);
  const indCount = Object.values(indicators).reduce((a, b) => a + (Array.isArray(b) ? b.length : 0), 0);
  console.log(`[worker] indicators count=${indCount}`);

  const macro = await buildMacro(prev?.macro || null, startedAt);

  let newLivewire = [];

  for (const region of REGIONS) {
    console.log(`[worker] region=${region} rss gather...`);
    let rssItems = [];
    try {
      rssItems = await gatherRegionNews(region);
    } catch (e) {
      console.warn(`[worker] RSS gather failed region=${region}:`, e?.message || e);
    }

    // quick visibility: did sector feeds return anything?
    const rssByHint = {};
    for (const r of rssItems) {
      const k = String(r.hintSector || "GENERAL").toUpperCase();
      rssByHint[k] = (rssByHint[k] || 0) + 1;
    }

    debug.regions[region] = {
      rss_count: rssItems.length,
      rss_by_hint: rssByHint,
      base_count: 0,
      ttl_count: 0,
      lookback_count: 0,
      rewrite_requested: 0,
      rewrite_used: 0,
      livewire_out: 0,
    };

    const now = new Date();
    const freshBase = rssItems
      .map(x => makeBaseItem({
        region,
        headline: x.title,
        link: x.link,
        publishedAt: safeDate(x.pubDate)?.toISOString() || nowIso(),
        description: x.description,
        sourceName: x.sourceName || "Source",
        sourceUrl: x.sourceUrl || x.link,
        hintSector: x.hintSector,
      }))
      .filter(isTrustedItem)
      .filter(x => withinHours(x.publishedAt, TTL_HOURS, now));

    // distribution after trust+TTL filter
    const baseBySector = {};
    for (const it of freshBase) {
      const s = String(it.sector || "GENERAL").toUpperCase();
      baseBySector[s] = (baseBySector[s] || 0) + 1;
    }
    debug.regions[region].base_by_sector = baseBySector;

    debug.regions[region].base_count = freshBase.length;

    const ttlItems = freshBase; // candidates within TTL window
    const lookbackItems = ttlItems.filter(x => withinHours(x.publishedAt, LOOKBACK_HOURS, now) || LOOKBACK_HOURS >= TTL_HOURS);
    debug.regions[region].ttl_count = ttlItems.length;
    debug.regions[region].lookback_count = lookbackItems.length;

    const prevRegionAll = prevLivewire.filter(x => x.region === region);
    const prevStories = collapseByStoryKey(prevRegionAll);
    const prevMap = new Map(prevStories.map(x => [x.storyKey, x]));

    // Only rewrite items that are new (or previously had no decent story), within LOOKBACK window.
    const toRewrite = lookbackItems
      .filter(x => {
        const p = prevMap.get(x.storyKey);
        if (!p) return true;
        if (p.llm) return false;
        return !p.story || p.story.length < 120;
      })
      .slice(0, GEMINI_MAX_NEW);

    debug.regions[region].rewrite_requested = toRewrite.length;

    let rewritten = toRewrite;
    if (toRewrite.length && ai) {
      console.log(`[worker] region=${region} gemini rewrite count=${toRewrite.length}`);
      rewritten = await rewriteItemsWithGemini(region, toRewrite);
    }
    debug.regions[region].rewrite_used = rewritten.length;

    const rewriteMap = new Map(rewritten.map(x => [x.storyKey, x]));

    // Keep ALL TTL items for Livewire. Prefer rewritten, then prior snapshot (if richer), then base.
    const ttlFinal = ttlItems.map(it => rewriteMap.get(it.storyKey) || prevMap.get(it.storyKey) || it);

    const combined = mergeByStoryKey(prevStories, ttlFinal);
    const ttlFiltered = trimLivewire(combined, startedAt);

    for (const sector of SECTORS) {
      if (sector === "GENERAL") continue;
      const items = ttlFiltered.filter(x => x.sector === sector || (Array.isArray(x.sectors) && x.sectors.includes(sector)));
      const selected = enforceQuota(items, SECTOR_MAX);
      newLivewire.push(...selected.map(x => ({ ...x, id: `${x.storyKey}:${region}:${sector}`, sector })));
    }

    // GENERAL is an aggregated view across all sectors (not only items labeled GENERAL).
    if (ttlFiltered.length) {
      newLivewire.push(
        ...enforceQuota(ttlFiltered, Math.max(8, Math.min(GENERAL_MAX, 24)))
          .map(x => ({ ...x, id: `${x.storyKey}:${region}:GENERAL`, sector: "GENERAL" }))
      );
    }

    debug.regions[region].livewire_out = newLivewire.filter(x => x.region === region).length;
    console.log(`[worker] region=${region} livewire_out=${debug.regions[region].livewire_out}`);
  }

  const mergedLivewire = trimLivewire(mergeLivewireByKey(prevLivewire, newLivewire), startedAt);

  const briefings = { regions: {} };
  for (const region of REGIONS) {
    briefings.regions[region] = {};
    for (const sector of SECTORS) {
      const items = mergedLivewire.filter(x => x.region === region && (sector === "GENERAL" ? true : (x.sector === sector || (Array.isArray(x.sectors) && x.sectors.includes(sector)))))
        .sort((a, b) => {
          const ia = impactRank(a.impact), ib = impactRank(b.impact);
          if (ia !== ib) return ib - ia;
          return (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0);
        });
      briefings.regions[region][sector] = buildDigest(region, sector, items, startedAt.toISOString());
    }
  }

  const morningBriefs = prev?.morningBriefs || prev?.morning_briefs || {};
  const mbMeta = prev?.mbMeta || {};

  for (const region of REGIONS) {
    const { hour, minute } = localHourMinute(region, startedAt);
    const releaseHour = MORNING_RELEASE_HOUR[region];
    const todayKey = localDateKey(region, startedAt);

    const shouldGenerate =
      (RUN_MODE === "morning") ||
      (RUN_MODE === "full") ||
      (hour > releaseHour || (hour === releaseHour && minute >= 0));

    const already = mbMeta[region] === todayKey;

    if (shouldGenerate && !already) {
      console.log(`[worker] morning brief generate region=${region} local=${todayKey} ${hour}:${String(minute).padStart(2, "0")}`);
      try {
        const regionItemsMixed = mergedLivewire.filter(x => x.region === region)
          .sort((a, b) => {
            const ia = impactRank(a.impact), ib = impactRank(b.impact);
            if (ia !== ib) return ib - ia;
            return (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0);
          })
          .slice(0, Math.max(12, GENERAL_UI_MAX));

        const mbText = await genMorningBriefText(region, regionItemsMixed);
        morningBriefs[region] = buildMorningDeck(region, regionItemsMixed, indicators?.[region] || [], mbText);
        mbMeta[region] = todayKey;
      } catch (e) {
        console.warn(`[worker] morning brief failed region=${region}:`, e?.message || e);
      }
    }
  }

  const out = {
    schema_version: 2,
    generatedAt: startedAt.toISOString(),
    generated_at_wib: new Intl.DateTimeFormat("id-ID", { timeZone: "Asia/Jakarta", dateStyle: "medium", timeStyle: "short" }).format(startedAt),
    status: { ok: true, mode: RUN_MODE, debug },
    macro,
        indices: indicators,
    indicators: indicators,
    livewire: mergedLivewire,
    briefings,
    morningBriefs,
    mbMeta,
  };

  await writeSnapshot(out);
  console.log(`[worker] RUN done livewire=${mergedLivewire.length}`);
}

main().catch((e) => {
  console.error("[worker] fatal:", e);
  process.exit(1);
});
