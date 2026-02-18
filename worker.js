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
const LOOKBACK_HOURS = Number(process.env.LOOKBACK_HOURS || (RUN_MODE === "full" ? 24 : 3));
const TTL_HOURS = Number(process.env.TTL_HOURS || 24);
// How wide RSS discovery is. Keep this wide (default: TTL) so first run is not empty.
const RSS_WINDOW_HOURS = Number(process.env.RSS_WINDOW_HOURS || TTL_HOURS);

// Gemini (optional)
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || process.env.API_KEY || "";
const GEMINI_MODEL = process.env.GEMINI_MODEL || ""; // if empty, auto-fallback list
const GEMINI_MAX_NEW = Number(process.env.GEMINI_MAX_NEW || 14); // max stories rewritten per region per run
const GEMINI_MAX_TOKENS = Number(process.env.GEMINI_MAX_TOKENS || 8192);
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

// Morning brief schedule (local time per region) — weekdays only
const MORNING_RELEASE_HOUR = {
  INDONESIA: 8,    // 08:00 WIB
  USA: 20,         // 20:30 WIB → mapped to ET below
  ASIA: 7,         // 07:30 HKT
  EUROPE: 7,       // 07:00 London
};
const MORNING_RELEASE_MINUTE = {
  INDONESIA: 0,
  USA: 30,
  ASIA: 30,
  EUROPE: 0,
};

// RSS Search presets (Google News RSS Search)
// Notes:
// - baseQ is for GENERAL discovery.
// - anchor is for sector discovery (keeps queries region-relevant without over-constraining to market terms).
const RSS_CFG = {
  INDONESIA: {
    hl: "id", gl: "ID", ceid: "ID:id",
    baseQ: "Indonesia (ekonomi OR bisnis OR keuangan OR saham OR IHSG OR rupiah OR investasi OR pasar)",
    anchor: "(Indonesia OR IHSG OR IDX OR rupiah OR saham OR bisnis OR ekonomi)"
  },
  USA: {
    hl: "en", gl: "US", ceid: "US:en",
    baseQ: "(economy OR stocks OR markets OR earnings OR Fed OR inflation OR \"Wall Street\" OR business)",
    anchor: "(US OR American OR \"Wall Street\" OR economy OR market OR business)"
  },
  ASIA: {
    hl: "en", gl: "HK", ceid: "HK:en",
    baseQ: "(Asia OR China OR Japan OR Korea OR India OR \"Hong Kong\" OR Taiwan) (economy OR stocks OR markets OR trade OR business)",
    anchor: "(Asia OR China OR Japan OR Korea OR India OR \"Hong Kong\" OR Taiwan OR ASEAN)"
  },
  EUROPE: {
    hl: "en", gl: "GB", ceid: "GB:en",
    baseQ: "(Europe OR EU OR UK OR Germany OR France) (economy OR stocks OR markets OR ECB OR business OR trade)",
    anchor: "(Europe OR EU OR UK OR Germany OR France OR ECB OR FTSE)"
  },
};

const SECTOR_RSS_HINTS = {
  TECHNOLOGY: '(tech OR technology OR teknologi OR AI OR "artificial intelligence" OR semiconductor OR chip OR GPU OR cloud OR software OR cybersecurity OR Apple OR Google OR Microsoft OR Nvidia OR startup OR digital OR robot)',
  FINANCE: '(bank OR banking OR "central bank" OR Fed OR ECB OR "interest rate" OR yield OR bond OR treasury OR stocks OR shares OR "stock market" OR earnings OR profit OR IPO OR forex OR currency OR inflation OR recession OR "Wall Street" OR investor)',
  MINING_ENERGY: '(oil OR crude OR Brent OR WTI OR OPEC OR gas OR LNG OR energy OR coal OR mining OR nickel OR copper OR gold OR silver OR lithium OR battery OR renewable OR solar OR "electric vehicle" OR EV)',
  HEALTHCARE: '(health OR healthcare OR hospital OR pharma OR pharmaceutical OR drug OR vaccine OR biotech OR medical OR FDA OR WHO OR pandemic OR cancer OR clinical OR "public health")',
  REGULATION: '(regulation OR policy OR law OR tariff OR tax OR sanction OR ban OR antitrust OR SEC OR DOJ OR FTC OR government OR legislation OR ruling OR "trade war" OR embargo OR compliance)',
  CONSUMER: '(consumer OR retail OR "e-commerce" OR ecommerce OR travel OR tourism OR airline OR hotel OR restaurant OR food OR automotive OR car OR "consumer spending" OR sales OR shopping OR luxury)',
};

// Allowlist for source domains. Google News RSS is the primary feed, and it already
// curates reputable sources. This list is a SUPPLEMENT, not a gate — see isTrustedItem.
const TRUSTED_DOMAINS = [
  // === Wire services & top-tier global ===
  "reuters.com", "apnews.com", "bloomberg.com", "wsj.com", "ft.com", "economist.com",
  "nytimes.com", "washingtonpost.com",
  // === US business / markets ===
  "cnbc.com", "marketwatch.com", "finance.yahoo.com", "investing.com", "barrons.com",
  "forbes.com", "fortune.com", "businessinsider.com", "insider.com", "benzinga.com",
  "seekingalpha.com", "fool.com", "thestreet.com", "zacks.com",
  // === US general ===
  "cnn.com", "cbsnews.com", "nbcnews.com", "abcnews.go.com", "foxbusiness.com",
  "npr.org", "politico.com", "axios.com", "semafor.com", "vox.com",
  // === Tech ===
  "techcrunch.com", "theverge.com", "wired.com", "arstechnica.com", "engadget.com",
  "zdnet.com", "cnet.com", "thenextweb.com", "venturebeat.com", "protocol.com",
  "tomshardware.com", "theinformation.com",
  // === UK / Europe ===
  "theguardian.com", "bbc.co.uk", "bbc.com", "telegraph.co.uk", "independent.co.uk",
  "sky.com", "skynews.com.au", "euronews.com", "dw.com", "france24.com", "politico.eu",
  // === Asia-Pacific ===
  "nikkei.com", "asia.nikkei.com", "scmp.com", "straitstimes.com", "channelnewsasia.com",
  "bangkokpost.com", "japantimes.co.jp", "koreaherald.com", "koreatimes.co.kr",
  "theedgemarkets.com", "thestar.com.my", "inquirer.net", "rappler.com",
  "livemint.com", "economictimes.indiatimes.com", "moneycontrol.com", "ndtv.com",
  "abc.net.au", "afr.com", "smh.com.au",
  // === Middle East / Africa ===
  "aljazeera.com", "arabnews.com", "gulfnews.com", "thenationalnews.com",
  // === Indonesia ===
  "cnbcindonesia.com", "bisnis.com", "kontan.co.id", "kompas.com", "kompas.id",
  "detik.com", "tempo.co", "katadata.co.id", "antaranews.com", "thejakartapost.com",
  "jakartaglobe.id", "investor.id", "republika.co.id", "idnfinancials.com",
  "tirto.id", "kumparan.com", "suara.com", "medcom.id", "liputan6.com",
  "okezone.com", "merdeka.com", "viva.co.id", "iNews.id",
  // === Crypto / fintech ===
  "coindesk.com", "cointelegraph.com", "theblock.co", "decrypt.co",
  // === Data & calendars ===
  "tradingeconomics.com", "tradingview.com",
  // === Energy / commodities ===
  "oilprice.com", "rigzone.com", "spglobal.com", "mining.com", "kitco.com",
  // === Healthcare ===
  "statnews.com", "fiercepharma.com", "medscape.com",
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
  return String(s || "")
    .replace(/&nbsp;/g, " ")
    .replace(/\u00A0/g, " ")   // non-breaking space unicode
    .replace(/&#160;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripHtml(s) {
  return String(s || "")
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/\u00A0/g, " ")
    .replace(/&#160;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
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
  // Wire / top-tier
  "reuters","bloomberg","cnbc","financial times","ft.com","wall street journal","wsj","marketwatch",
  "the economist","nikkei","associated press","ap news","ap",
  // US business
  "barron","forbes","fortune","semafor","axios","benzinga","seeking alpha","motley fool",
  "the street","business insider","insider","fox business","yahoo finance","investing.com",
  // US general
  "cnn","nbc","abc news","cbs","npr","politico","vox","new york times","washington post",
  // Tech
  "the verge","techcrunch","wired","ars technica","engadget","zdnet","cnet","venturebeat",
  // UK/Europe
  "the guardian","bbc","sky news","telegraph","euronews","dw","france24","france 24","politico eu",
  // Asia-Pacific
  "nikkei asia","south china morning post","scmp","straits times","channel news asia","cna",
  "bangkok post","japan times","korea herald","korea times","the edge",
  "the star","inquirer","rappler","livemint","economic times","moneycontrol","ndtv",
  // Middle East
  "al jazeera","arab news","gulf news",
  // Indonesia
  "kompas","bisnis indonesia","bisnis.com","kontan","cnbc indonesia","tempo","antara","katadata",
  "the jakarta post","jakarta post","detik","tirto","kumparan","investor","republika",
  "liputan6","okezone","medcom","merdeka","suara","inews","idn","idnfinancials",
  // Energy / commodities
  "oilprice","oil price","rigzone","s&p global","kitco","mining.com",
  // Healthcare
  "stat news","statnews","fierce pharma","medscape",
  // Crypto
  "coindesk","cointelegraph","the block","decrypt",
];

function isTrustedSourceName(name) {
  if (!name) return false;
  const n = String(name).toLowerCase().trim();
  return TRUSTED_SOURCE_NAME_HINTS.some(h => n.includes(h));
}

// Spam / low-quality domains to explicitly reject even from Google News
const BLOCKED_DOMAINS = [
  "blogspot.com", "wordpress.com", "medium.com", "substack.com",
  "reddit.com", "twitter.com", "x.com", "facebook.com", "tiktok.com",
  "pinterest.com", "quora.com",
];

function isBlockedUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    return BLOCKED_DOMAINS.some(d => u.hostname.endsWith(d));
  } catch { return false; }
}

function isTrustedItem(item) {
  const sources = Array.isArray(item?.sources) ? item.sources : [];
  const primaryUrl = sources[0]?.url || "";
  const primaryName = sources[0]?.name || "";

  // Reject explicitly blocked domains
  for (const s of sources) {
    if (isBlockedUrl(s?.url || "")) return false;
  }

  // Trust items from known reputable domains
  for (const s of sources) {
    const url = s?.url || "";
    try {
      const u = new URL(url);
      if (!u.hostname.endsWith("news.google.com") && isTrustedUrl(url)) return true;
    } catch {}
  }

  // Trust items with a recognized source name
  if (isTrustedSourceName(primaryName)) return true;

  // Google News RSS already curates reputable sources. If item comes from
  // Google News and has ANY source name, trust it. This dramatically increases
  // coverage for legitimate outlets not in our allowlist.
  try {
    const u = new URL(primaryUrl);
    if (u.hostname.endsWith("news.google.com") && primaryName && primaryName.length > 1) return true;
  } catch {}

  // Accept any item that has a real publisher URL (not Google redirect) with a source name
  if (primaryName && primaryName.length > 1 && primaryUrl) return true;

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
    "tech","technology","teknologi","digital","digitalisasi",
    "ai","artificial intelligence","kecerdasan buatan","machine learning","deep learning",
    "semiconductor","semikonduktor","chip","chipset","gpu","cpu","nvidia","amd","tsmc","intel","qualcomm","broadcom","asml",
    "software","saas","cloud","aws","azure","gcp","cybersecurity","ransomware","data breach","siber",
    "smartphone","iphone","android","handphone","gadget","telecom","telekomunikasi","telkom","indosat","5g","internet",
    "startup","venture","unicorn","fintech","edtech",
    "robot","robotik","automation","otomasi","quantum","computing",
    "openai","chatgpt","gemini","anthropic","meta","google","apple","microsoft","amazon","samsung","sony",
    "aplikasi","platform","blockchain","crypto","bitcoin","ethereum","kripto",
    "streaming","netflix","tiktok","gopay","dana","ovo","tokopedia"
  ],
  FINANCE: [
    "bank","banking","perbankan","keuangan","finansial","financial",
    "bca","bri","bni","mandiri","bsi","cimb","bbca","bbri","bbni","bmri",
    "central bank","bank sentral","fed","fomc","ecb","boj","boe","rba","pboc","bi rate",
    "bank indonesia","ojk","fdic",
    "interest rate","suku bunga","rate cut","rate hike","dovish","hawkish",
    "yield","yields","bond","bonds","obligasi","treasury","t-bill","sbn","sukuk",
    "inflation","inflasi","deflasi","cpi","ppi","unemployment","nfp","gdp","pdb","growth","resesi","recession",
    "fx","forex","currency","mata uang","dollar","dolar","usd","eur","jpy","rupiah","idr","usd/idr","dxy","valas",
    "saham","stocks","stock","shares","equity","equities","investasi","investment","investor","reksadana",
    "index","indices","indeks","bursa","ihsg","idx","lq45","nasdaq","s&p","dow","wall street","nikkei","hang seng","kospi",
    "trading","perdagangan","sell-off","rally","bullish","bearish","koreksi","correction",
    "market","markets","pasar","pasar modal","risk-on","risk-off","capital flow","outflow","inflow",
    "asing","foreign","net buy","net sell",
    "earnings","profit","laba","rugi","loss","pendapatan","revenue","guidance","dividend","dividen","buyback","ipo","rights issue",
    "asuransi","insurance","leasing","pinjaman","kredit","loan","mortgage"
  ],
  MINING_ENERGY: [
    "oil","crude","brent","wti","opec","opec+","minyak","minyak mentah",
    "gas","lng","natural gas","gas alam","diesel","fuel","bbm","pertamina",
    "energy","energi","power","electricity","listrik","pln","utility","utilities",
    "ebt","renewable","terbarukan","solar","wind","geothermal",
    "coal","batubara","batu bara","adaro","itmg","ptba",
    "mining","tambang","pertambangan","minerals","mineral","smelter","hilirisasi","refinery",
    "nickel","nikel","copper","tembaga","tin","timah","bauxite","bauksit","aluminium",
    "gold","emas","silver","perak","platinum","palladium","antam",
    "ev","electric vehicle","kendaraan listrik","baterai","battery","lithium","rare earth","cobalt",
    "freeport","vale","bukit asam","medco"
  ],
  HEALTHCARE: [
    "health","healthcare","kesehatan","hospital","rumah sakit","rumahsakit","clinic","klinik",
    "pharma","pharmaceutical","farmasi","drug","obat","vaccine","vaksin","biotech","biotek","bioteknologi",
    "medical","medis","kedokteran","doctor","dokter","nurse","perawat","bpjs","jkn",
    "kalbe","kimia farma","bio farma","indofarma",
    "who","pandemic","pandemi","endemic","outbreak","wabah","virus","covid",
    "fda","bpom","clinical trial","uji klinis",
    "cancer","kanker","disease","penyakit","asuransi kesehatan","insurance health"
  ],
  REGULATION: [
    "regulation","regulasi","regulator","policy","kebijakan","law","hukum","undang-undang","peraturan",
    "government","pemerintah","kementerian","ministry",
    "sanction","sanksi","embargo","ban","larangan","tariff","tarif","bea masuk",
    "tax","pajak","ppn","pph","cukai","fiskal","fiscal","apbn","budget",
    "subsidy","subsidi","insentif","incentive","stimulus",
    "antitrust","competition","monopoly","persaingan","merger","akuisisi","acquisition",
    "probe","investigation","penyelidikan","korupsi","corruption","kpk",
    "sec","doj","ftc","eu commission","ojk","bappebti","kppu","kemenkeu","kementerian keuangan",
    "dpr","presiden","president","menteri","minister",
    "election","pemilu","geopolitik","geopolitical","trade war","perang dagang",
    "ruling","verdict","putusan","court","pengadilan"
  ],
  CONSUMER: [
    "consumer","konsumen","retail","ritel","fmcg","consumer goods","e-commerce","ecommerce","marketplace",
    "shopping","belanja","sales","penjualan","spending","pengeluaran","konsumsi","daya beli",
    "travel","tourism","pariwisata","wisata","airline","maskapai","garuda","hotel","restaurant","restoran",
    "food","makanan","beverage","minuman","grocery","supermarket","indomaret","alfamart",
    "unilever","indofood","mayora",
    "automotive","otomotif","car","mobil","motorcycle","motor","toyota","honda","astra",
    "ride-hailing","gojek","grab","shopee","traveloka","lazada",
    "properti","property","real estate","luxury","fashion"
  ],
};

// Capital market relevance filter — reject news that has no bearing on markets/investing.
// These are NEGATIVE keywords: if headline matches ONLY these and nothing else, skip.
const NOT_MARKET_RELEVANT = [
  // Lifestyle / entertainment / religion
  "resep", "recipe", "liburan", "mudik", "lebaran", "imlek", "natal", "tahun baru",
  "wisata kuliner", "destinasi", "tips liburan", "promo hotel",
  "gossip", "selebrit", "artis", "film", "drama", "konser", "musik",
  "olahraga", "sepak bola", "liga", "piala", "badminton", "tennis",
  // Personal finance tips (not market-moving)
  "tips cuan", "strategi cuan", "cara investasi pemula", "cara nabung",
  "ide bisnis", "bisnis rumahan", "kerja sampingan", "usaha kecil",
  // Crime / accidents (unless financial)
  "kecelakaan", "banjir", "gempa", "tsunami", "kebakaran", "pembunuhan",
  "cuaca", "hujan", "panas", "suhu",
  // Health/wellness (not pharma/biotech)
  "diet", "olahraga sehat", "tips sehat", "gizi", "kecantikan",
];

function isMarketRelevant(headline, description) {
  const t = normText(`${headline || ""} ${description || ""}`);
  if (!t) return false;

  // If text matches any SECTOR_KEYWORDS, it's relevant
  for (const kws of Object.values(SECTOR_KEYWORDS)) {
    for (const kw of kws) {
      if (hasToken(t, kw)) return true;
    }
  }

  // Check negative list — if headline is dominated by non-market topics, reject
  let negScore = 0;
  for (const neg of NOT_MARKET_RELEVANT) {
    if (t.includes(neg.toLowerCase())) negScore++;
  }
  if (negScore >= 2) return false;

  // Default: allow through (might still be relevant)
  return true;
}

// Impact classification — strict criteria for capital-market relevance.
// HIGH: macro-level events that directly move the entire market
//   (central bank decisions, GDP, CPI, geopolitical shocks, major regulation)
// MEDIUM: micro/sector-level events (corporate action, sector news, currency moves, earnings)
// LOW: everything else (general news, soft stories, minor updates)
// HIGH = ONLY whole-market macro shocks. Must be very selective.
const IMPACT_HIGH = [
  // Central bank DECISIONS (not just mentions)
  "rate cut", "rate hike", "rate decision", "rate hold",
  "fomc decision", "fomc statement", "fed cut", "fed hike",
  "bi rate decision", "bi raise", "bi cut",
  // Macro shocks / crashes
  "recession confirm", "debt default", "market crash", "circuit breaker",
  "flash crash", "black monday", "black swan",
  "gdp contract", "gdp shrink", "gdp negative",
  // Geopolitical crises
  "invasion", "military strike", "trade war escalat", "tariff war",
  "embargo", "martial law", "capital control", "debt ceiling breach",
  // Emergency regulation
  "emergency decree", "emergency regulation", "market halt",
];
// MEDIUM = corporate events, sector shifts, data releases, currency moves
const IMPACT_MED  = [
  // Corporate / micro
  "earnings", "revenue beat", "revenue miss", "profit", "guidance",
  "merger", "acquisition", "takeover", "ipo", "rights issue", "buyback", "dividend",
  "corporate action", "stock split", "delisting",
  // Currency / rates
  "forex", "exchange rate", "yield", "bond", "treasury",
  "rupiah", "dollar", "dxy",
  // Sector-level
  "oil price", "opec", "commodity", "coal price", "nickel price", "gold price",
  // Legal / probe
  "lawsuit", "probe", "investigation", "sec charge", "fraud",
  // Macro data (these are MEDIUM, not HIGH)
  "nfp", "non-farm", "nonfarm", "unemployment rate",
  "cpi", "ppi", "inflation", "gdp",
  "pmi", "consumer confidence", "retail sales", "industrial production",
  "trade balance", "jolts", "initial claims", "housing",
  // Central bank commentary (not decisions)
  "dovish", "hawkish", "fed fund", "fomc minute", "bank indonesia",
  "ecb rate", "boj rate", "boe rate", "pboc",
  // Geopolitical (not crisis level)
  "sanction", "tariff", "trade war", "war",
];

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
  const t = normText(text);
  if (!t) return "LOW";
  if (IMPACT_HIGH.some(k => hasToken(t, k))) return "HIGH";
  if (IMPACT_MED.some(k => hasToken(t, k))) return "MEDIUM";
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

// Extract real article URL from Google News RSS <description> HTML.
// Google News RSS description contains: <a href="https://real-article-url.com/...">headline</a>
function extractArticleUrlFromDesc(descHtml) {
  if (!descHtml) return "";
  const match = String(descHtml).match(/href="([^"]+)"/);
  if (!match) return "";
  const url = canonicalUrl(match[1]);
  // Skip if it's still a Google News URL
  try {
    const u = new URL(url);
    if (u.hostname.endsWith("news.google.com")) return "";
  } catch { return ""; }
  return url;
}

// Fetch article text content from a URL. Returns first ~2000 chars of article body.
// Uses concurrency-safe timeout and graceful fallback.
const ARTICLE_FETCH_TIMEOUT = 8000;
async function fetchArticleText(url) {
  if (!url) return "";
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ARTICLE_FETCH_TIMEOUT);
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,*/*",
      },
      redirect: "follow",
    });
    clearTimeout(timer);
    if (!res.ok) return "";
    const contentType = res.headers.get("content-type") || "";
    if (!contentType.includes("html") && !contentType.includes("text")) return "";
    const html = await res.text();
    return extractTextFromHtml(html);
  } catch {
    return "";
  }
}

/**
 * Extract og:image (or twitter:image, or first large <img>) from HTML.
 * Returns a URL string or "".
 */
function extractImageFromHtml(html) {
  if (!html) return "";
  // 1. og:image meta tag (most reliable)
  const ogMatch = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i)
    || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i);
  if (ogMatch?.[1]) return ogMatch[1].trim();

  // 2. twitter:image meta tag
  const twMatch = html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i)
    || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter:image["']/i);
  if (twMatch?.[1]) return twMatch[1].trim();

  // 3. First large image in <article> or <main>
  const body = html.match(/<article[^>]*>([\s\S]*?)<\/article>/i)?.[1]
    || html.match(/<main[^>]*>([\s\S]*?)<\/main>/i)?.[1]
    || "";
  if (body) {
    const imgMatch = body.match(/<img[^>]+src=["']([^"']+)["'][^>]*>/i);
    if (imgMatch?.[1]) {
      const src = imgMatch[1].trim();
      // Only use if it looks like a real image URL (not tiny icons/tracking pixels)
      if (/\.(jpg|jpeg|png|webp)/i.test(src) && !/(icon|logo|avatar|pixel|1x1|badge)/i.test(src)) {
        return src;
      }
    }
  }
  return "";
}

async function fetchArticleData(url) {
  if (!url) return { text: "", imageUrl: "" };
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ARTICLE_FETCH_TIMEOUT);
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,*/*",
      },
      redirect: "follow",
    });
    clearTimeout(timer);
    if (!res.ok) return { text: "", imageUrl: "" };
    const contentType = res.headers.get("content-type") || "";
    if (!contentType.includes("html") && !contentType.includes("text")) return { text: "", imageUrl: "" };
    const html = await res.text();
    return {
      text: extractTextFromHtml(html),
      imageUrl: extractImageFromHtml(html),
    };
  } catch {
    return { text: "", imageUrl: "" };
  }
}

// Extract readable text from HTML article page.
// Prioritizes <article>, <main>, then falls back to all <p> tags.
function extractTextFromHtml(html) {
  if (!html) return "";

  // Try to find article body (most news sites wrap content in <article> or specific divs)
  let body = "";

  // Strategy 1: Extract from <article>...</article>
  const articleMatch = html.match(/<article[^>]*>([\s\S]*?)<\/article>/i);
  if (articleMatch) {
    body = articleMatch[1];
  }

  // Strategy 2: If no <article>, try <main>
  if (!body) {
    const mainMatch = html.match(/<main[^>]*>([\s\S]*?)<\/main>/i);
    if (mainMatch) body = mainMatch[1];
  }

  // Strategy 3: fall back to full HTML
  if (!body) body = html;

  // Extract text from <p> tags only (most reliable for article content)
  const paragraphs = [];
  const pRegex = /<p[^>]*>([\s\S]*?)<\/p>/gi;
  let m;
  while ((m = pRegex.exec(body)) !== null) {
    const text = stripHtml(m[1]).trim();
    // Filter out very short paragraphs (nav items, captions) and very long ones (embedded data)
    if (text.length >= 40 && text.length < 3000) {
      paragraphs.push(text);
    }
  }

  if (!paragraphs.length) return "";

  // Join paragraphs and limit to ~2000 chars
  let result = paragraphs.join("\n\n");
  if (result.length > 2000) result = result.slice(0, 2000);
  return result;
}

// Enrich items with real article URLs, content, and images, with concurrency control.
const ARTICLE_CONCURRENCY = 5;
async function enrichItemsWithArticleContent(items) {
  const textResults = new Array(items.length);
  const imageResults = new Array(items.length);

  // Process in batches of ARTICLE_CONCURRENCY
  for (let i = 0; i < items.length; i += ARTICLE_CONCURRENCY) {
    const batch = items.slice(i, i + ARTICLE_CONCURRENCY);
    const batchResults = await Promise.allSettled(
      batch.map(async (item) => {
        const articleUrl = item.articleUrl || item.sources?.[0]?.url || "";
        if (!articleUrl) return { text: "", imageUrl: "" };
        return fetchArticleData(articleUrl);
      })
    );

    for (let j = 0; j < batchResults.length; j++) {
      const idx = i + j;
      const val = batchResults[j].status === "fulfilled" ? batchResults[j].value : {};
      textResults[idx] = val.text || "";
      imageResults[idx] = val.imageUrl || "";
    }
  }

  // Attach imageUrl directly to items for downstream use
  for (let i = 0; i < items.length; i++) {
    if (imageResults[i]) {
      items[i].imageUrl = imageResults[i];
    }
  }

  return textResults;
}

function makeBaseItem({ region, headline, link, publishedAt, description, sourceName, sourceUrl, articleUrl, hintSector }) {
  const url = canonicalUrl(sourceUrl || link);
  const storyKey = sha1(`${url}|${headline}`);
  const snippet = cleanText(stripHtml(description || ""));
  const cleanHeadline = cleanText(headline);

  // Build a meaningful fallback story from keypoints + description
  const kp = fallbackKeypoints(description);
  let fallbackStory = "";
  if (kp.length) {
    fallbackStory = kp.join(". ");
    if (!fallbackStory.endsWith(".")) fallbackStory += ".";
  } else if (snippet && snippet.toLowerCase() !== cleanHeadline.toLowerCase()) {
    fallbackStory = snippet.slice(0, 650);
  }

  const hinted = (hintSector && SECTORS.includes(String(hintSector).toUpperCase())) ? String(hintSector).toUpperCase() : null;

  let sectors;
  try {
    sectors = inferSectors(`${headline} ${description || ""}`);
  } catch {
    sectors = hinted && hinted !== "GENERAL" ? [hinted] : ["GENERAL"];
  }

  if (hinted && hinted !== "GENERAL") sectors = Array.from(new Set([hinted, ...sectors]));

  return {
    id: `${storyKey}:${region}`,
    storyKey,
    region,
    sector: (hinted && hinted !== "GENERAL") ? hinted : (sectors && sectors.length ? sectors[0] : "GENERAL"),
    sectors: (sectors && sectors.length ? sectors : undefined),
    impact: inferImpact(`${headline} ${description || ""}`),
    headline: cleanHeadline,
    keypoints: kp,
    story: fallbackStory,
    imageUrl: "", // populated later by enrichItemsWithArticleContent
    publishedAt,
    sources: [{ name: cleanText(sourceName || "Source"), url }],
    articleUrl: canonicalUrl(articleUrl || ""), // real article URL for content fetching
  };
}

function enforceQuota(items, maxTotal = SECTOR_MAX) {
  // Strict quota: HIGH max 4, MEDIUM max 4, LOW max 2 = total max 10.
  // For GENERAL view (maxTotal > 10), allow more but keep proportions.
  const isGeneral = maxTotal > 10;
  const want = isGeneral
    ? { HIGH: 6, MEDIUM: 8, LOW: 4 }   // GENERAL: up to 18
    : { HIGH: 4, MEDIUM: 4, LOW: 2 };   // Sector: strict 10

  const by = { HIGH: [], MEDIUM: [], LOW: [] };

  for (const it of items) {
    const k = String(it.impact || "LOW").toUpperCase();
    (by[k] || by.LOW).push(it);
  }

  for (const k of Object.keys(by)) {
    by[k].sort((a, b) => (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0));
  }

  const out = [];
  for (const k of ["HIGH", "MEDIUM", "LOW"]) out.push(...by[k].slice(0, want[k]));

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
  return arr.map(it => {
    const desc = it.description?.text ?? it.description;
    const link = canonicalUrl(it.link?.text ?? it.link);
    // Google News RSS: real article URL is in <a href="..."> inside description HTML
    const realUrl = extractArticleUrlFromDesc(desc);
    const rssSourceUrl = canonicalUrl(it.source?.url ?? "");
    return {
      title: cleanText(it.title?.text ?? it.title),
      link,
      pubDate: it.pubDate?.text ?? it.pubDate,
      description: desc,
      sourceName: cleanText(it.source?.text ?? it.source),
      // Prefer: real article URL from description > RSS source url > Google News redirect link
      sourceUrl: realUrl || rssSourceUrl || link,
      articleUrl: realUrl || "", // real article URL for content fetching
    };
  }).filter(x => x.title && x.link);
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

// Extra broad queries per region to increase discovery volume
const EXTRA_QUERIES = {
  INDONESIA: [
    { q: "Indonesia saham pasar modal IHSG investasi", hintSector: "FINANCE" },
    { q: "Indonesia teknologi startup digital", hintSector: "TECHNOLOGY" },
    { q: "Indonesia tambang energi minyak batubara nikel", hintSector: "MINING_ENERGY" },
    { q: "Indonesia regulasi pajak pemerintah kebijakan", hintSector: "REGULATION" },
    { q: "Indonesia konsumen retail otomotif e-commerce", hintSector: "CONSUMER" },
  ],
  USA: [
    { q: "US technology AI semiconductor Apple Google Nvidia", hintSector: "TECHNOLOGY" },
    { q: "US oil energy renewable electric vehicle", hintSector: "MINING_ENERGY" },
    { q: "US healthcare pharma FDA biotech drug", hintSector: "HEALTHCARE" },
    { q: "US regulation SEC antitrust tariff trade policy", hintSector: "REGULATION" },
    { q: "US consumer retail Amazon spending travel", hintSector: "CONSUMER" },
  ],
  ASIA: [
    { q: "Asia China Japan Korea technology semiconductor AI", hintSector: "TECHNOLOGY" },
    { q: "Asia China Japan Korea bank finance stock market", hintSector: "FINANCE" },
    { q: "Asia China oil energy mining commodities", hintSector: "MINING_ENERGY" },
    { q: "Asia China Japan regulation trade tariff policy", hintSector: "REGULATION" },
    { q: "Asia China Japan consumer retail travel tourism", hintSector: "CONSUMER" },
    { q: "China economy trade exports manufacturing", hintSector: "GENERAL" },
    { q: "Japan economy BOJ yen Nikkei", hintSector: "GENERAL" },
    { q: "India economy market Sensex Nifty RBI", hintSector: "GENERAL" },
  ],
  EUROPE: [
    { q: "Europe EU technology AI semiconductor", hintSector: "TECHNOLOGY" },
    { q: "Europe EU oil energy gas renewable", hintSector: "MINING_ENERGY" },
    { q: "Europe EU healthcare pharma drug", hintSector: "HEALTHCARE" },
    { q: "Europe EU consumer retail luxury automotive", hintSector: "CONSUMER" },
    { q: "UK economy Bank of England pound FTSE", hintSector: "GENERAL" },
    { q: "Germany economy DAX industry manufacturing", hintSector: "GENERAL" },
  ],
};

async function gatherRegionNews(region) {
  const cfg = RSS_CFG[region] || { baseQ: region, anchor: region };
  const base = cfg.baseQ || region;
  const anchor = cfg.anchor || region;

  // Discovery strategy:
  // - GENERAL feed: broad market query (baseQ)
  // - Sector feeds: (anchor) AND (sector hints) to force sector coverage.
  // - Extra feeds: broader standalone queries for each sector.
  // This avoids the classic failure where everything collapses into GENERAL.
  const queries = [
    { q: base, hintSector: "GENERAL" },
    ...Object.entries(SECTOR_RSS_HINTS).map(([sector, q]) => ({
      q: `(${anchor}) ${q}`,
      hintSector: sector
    })),
    ...(EXTRA_QUERIES[region] || []),
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

// Map region -> relevant currencies (ForexFactory uses country codes like USD, EUR etc.)
const REGION_CCY = {
  USA: ["USD"],
  EUROPE: ["EUR", "GBP", "CHF"],
  ASIA: ["JPY", "CNY", "HKD", "KRW", "AUD", "NZD"],
  // Indonesia: IDR rarely in ForexFactory; show USD + key drivers that move EM/IDR
  INDONESIA: ["USD", "CNY", "JPY", "IDR"],
};

const MACRO_KEYWORDS = [
  "Non-Farm", "NFP", "Unemployment", "CPI", "PPI", "GDP", "Retail Sales",
  "PMI", "Manufacturing", "Services", "Fed", "FOMC", "Rate", "Interest",
  "Core", "Inflation", "Payroll", "Jobless", "Confidence", "Trade Balance",
  "JOLTS", "ISM", "Housing", "Consumer", "Industrial", "Durable",
  "Personal Income", "Personal Spending", "PCE", "Employment",
  "Nonfarm", "Building Permits", "New Home", "Existing Home",
  "Initial Claims", "Continuing Claims", "ADP", "Michigan",
  "Import", "Export", "Current Account", "Budget", "Deficit",
  "Treasury", "Auction", "BOJ", "BOE", "ECB", "RBA", "PBOC",
  "BI Rate", "Bank Indonesia", "Flash", "Preliminary", "Final"
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

// Priority macro events — these are ALWAYS included if found
const MACRO_PRIORITY = [
  "Non-Farm", "NFP", "Nonfarm", "Unemployment Rate", "CPI", "PPI",
  "PMI", "Consumer Confidence", "JOLTS", "Fed Fund", "FOMC",
  "GDP", "PCE", "Retail Sales", "ISM",
  "BI Rate", "Bank Indonesia", "BOJ", "BOE", "ECB",
];

function macroInteresting(title, impact) {
  const t = String(title || "").toLowerCase();
  // Always include High impact events regardless of keyword match
  const imp = String(impact || "").toLowerCase();
  if (imp === "high") return true;
  // Priority events always included
  if (MACRO_PRIORITY.some(k => t.includes(k.toLowerCase()))) return true;
  // Medium impact + keyword match
  if (imp === "medium" && MACRO_KEYWORDS.some(k => t.includes(String(k).toLowerCase()))) return true;
  // Low impact — only include if it matches priority keywords
  return false;
}

// Forex Factory calendar URLs — try multiple in case one is down
const MACRO_CAL_URLS = [
  process.env.MACRO_CAL_URL || "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
  "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
];

async function fetchMacroCalendar() {
  // Try XML first
  for (const url of MACRO_CAL_URLS) {
    try {
      const res = await fetch(url, {
        headers: {
          "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          "accept": "*/*",
        },
      });
      if (!res.ok) { console.warn(`[worker] macro cal ${url} HTTP ${res.status}`); continue; }
      const text = await res.text();

      let events;
      if (url.endsWith(".json")) {
        // JSON format
        const arr = JSON.parse(text);
        events = (Array.isArray(arr) ? arr : []).map(e => ({
          title: cleanText(e.title || ""),
          country: cleanText(e.country || ""),
          currency: cleanText(e.country || ""), // JSON uses 'country' as currency code
          date: cleanText(e.date || ""),
          time: cleanText(e.time || ""),
          impact: cleanText(e.impact || ""),
          actual: normalizeMacroVal(e.actual),
          forecast: normalizeMacroVal(e.forecast),
          previous: normalizeMacroVal(e.previous),
          url: cleanText(e.url || ""),
        }));
      } else {
        // XML format
        const obj = parser.parse(text);
        // Try multiple XML structures
        const ev = obj?.weeklyevents?.event
          || obj?.weeklyevents?.events?.event
          || obj?.weeklyevents?.events
          || obj?.event
          || obj?.events?.event;
        if (!ev) { console.warn(`[worker] macro XML parsed but no events found. Keys:`, Object.keys(obj || {})); continue; }
        const arr = Array.isArray(ev) ? ev : [ev];
        events = arr.map(e => ({
          title: nodeText(e.title),
          country: nodeText(e.country),
          currency: nodeText(e.currency) || nodeText(e.country),
          date: nodeText(e.date),
          time: nodeText(e.time),
          impact: nodeText(e.impact),
          actual: normalizeMacroVal(nodeText(e.actual)),
          forecast: normalizeMacroVal(nodeText(e.forecast)),
          previous: normalizeMacroVal(nodeText(e.previous)),
          url: nodeText(e.url),
        }));
      }

      const valid = events.filter(x => x.title);
      console.log(`[worker] macro cal ${url}: ${valid.length} events parsed`);
      if (valid.length) return valid;
    } catch (e) {
      console.warn(`[worker] macro cal ${url} failed:`, e?.message || e);
    }
  }
  return [];
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
  events = events.filter(e => macroInteresting(e.title, e.impact));

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
    { code: "DJI", name: "Dow Jones", symbol: "^DJI" },
    { code: "RUT", name: "Russell 2000", symbol: "^RUT" },
    { code: "BTC", name: "Bitcoin", symbol: "BTC-USD" },
    { code: "VIX", name: "VIX", symbol: "^VIX" },
    { code: "DXY", name: "US Dollar Index", symbol: "DX-Y.NYB" },
    { code: "GOLD", name: "Gold", symbol: "GC=F" },
    { code: "OIL", name: "WTI Crude", symbol: "CL=F" },
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

    if (story && story.length >= 30) {
      out.story = story;
    } else if (!out.story || out.story.length < 30) {
      // last resort: build a minimal story from headline + keypoints
      const kps = (out.keypoints || []).filter(Boolean);
      const parts = [];
      if (out.headline) parts.push(out.headline + ".");
      if (kps.length) parts.push(kps.join(". ") + ".");
      out.story = parts.join(" ");
    }

    out.llm = true;
  }
  return out;
}

async function rewriteItemsWithGemini(region, items, articleTexts = []) {
  if (!ai) return items;
  if (!items.length) return items;

  const modelFallback = [
    GEMINI_MODEL,
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
  ].filter(Boolean);

  const payload = items.map((it, idx) => {
    const articleText = articleTexts[idx] || "";
    // Use article text if available, otherwise fall back to keypoints
    const description = articleText
      ? articleText.slice(0, 800)
      : (it.keypoints?.join(" ") || "");
    return {
      storyKey: it.storyKey,
      headline: it.headline,
      description,
      hasFullArticle: articleText.length > 100,
      publishedAt: it.publishedAt,
      sourceName: it.sources?.[0]?.name || "Source",
      sourceUrl: it.sources?.[0]?.url || "",
      suggestedSector: it.sector,
      suggestedImpact: it.impact,
    };
  });

  const prompt = `
Kamu adalah editor senior pasar finansial di terminal Bloomberg-style. Tugasmu menulis ulang berita menjadi laporan profesional.

INPUT (array):
${JSON.stringify(payload, null, 2)}

ATURAN WAJIB:
1. Setiap item HARUS memiliki "story" minimal 100 kata. Ini WAJIB — jangan pernah kosongkan field story.
2. Jika "hasFullArticle" true: gunakan description untuk membuat narasi kaya 150-200 kata.
3. Jika "hasFullArticle" false: kembangkan headline menjadi narasi informatif 100-150 kata. Jelaskan konteks pasar, dampak potensial, dan implikasi investor. Gunakan pengetahuanmu tentang pasar finansial untuk memberikan konteks yang relevan.
4. Keypoints: 3 poin BERBEDA dari headline, informatif & spesifik, masing-masing ±25 kata.
5. Jangan mengarang angka spesifik atau kutipan yang tidak ada di input.
6. Bahasa Indonesia profesional, terminal-style.
7. Output: valid JSON array (TANPA markdown code blocks).

ATURAN IMPACT (SANGAT KETAT — terlalu banyak HIGH = masalah):
- HIGH = HANYA keputusan resmi bank sentral (rate cut/hike/hold), crash pasar, resesi resmi, krisis geopolitik besar (invasi, embargo). Maksimal 1-2 per batch. Jika ragu, gunakan MEDIUM.
- MEDIUM = data makro (CPI, GDP, NFP), corporate action besar (M&A, IPO, earnings), pergerakan kurs/komoditas signifikan, kebijakan pemerintah
- LOW = berita umum, update rutin, opini, analisis, tips, awards, penghargaan

FORMAT OUTPUT — JSON array:
[{
  "storyKey": "...",
  "headline": "headline rapi (maks 110 char)",
  "sector": one of ${JSON.stringify(SECTORS)},
  "impact": "HIGH|MEDIUM|LOW",
  "keypoints": ["poin 1", "poin 2", "poin 3"],
  "story": "narasi profesional 150-200 kata. Bagi menjadi paragraf yang jelas (setiap paragraf ~5 kalimat). Pisahkan paragraf dengan newline ganda."
}]
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

  const input = topItems.slice(0, 12).map(it => ({
    headline: it.headline,
    sector: it.sector,
    impact: it.impact,
    keypoints: (it.keypoints || []).slice(0, 2),
    source: it.sources?.[0]?.name || ""
  }));

  // Region-specific prompts
  let prompt;
  if (region === "INDONESIA") {
    prompt = `
Kamu menulis Morning Briefing pasar Indonesia.
Aturan keras: Jangan mengarang fakta baru. Berdasarkan headline berikut. Output valid JSON tanpa markdown.

HEADLINE LIST:
${JSON.stringify(input, null, 2)}

OUTPUT JSON — isi SEMUA field, gunakan data dari headline:
{
  "title": "Judul ringkas (contoh: IHSG Terkoreksi di Tengah Tekanan Asing)",
  "lede": "1 kalimat pembuka yang merangkum kondisi pasar hari ini (50-80 kata)",
  "flow": {
    "foreignBuy": "estimasi nominal (contoh: Rp 2.1T)",
    "foreignSell": "estimasi nominal (contoh: Rp 3.4T)",
    "domesticBuy": "estimasi nominal",
    "domesticSell": "estimasi nominal",
    "foreignNet": { "valueFmt": "net (contoh: -Rp 1.3T)" },
    "domesticNet": { "valueFmt": "net (contoh: +Rp 1.3T)" },
    "topBuys": [{"ticker":"BBCA","valueFmt":"Rp 450M"},{"ticker":"BMRI","valueFmt":"Rp 320M"},{"ticker":"TLKM","valueFmt":"Rp 280M"}],
    "topSells": [{"ticker":"BBRI","valueFmt":"Rp 520M"},{"ticker":"ASII","valueFmt":"Rp 310M"},{"ticker":"UNVR","valueFmt":"Rp 180M"}],
    "miniNote": "IHSG turun/naik X.XX% ke level XXXX didorong oleh ... (1 kalimat)"
  },
  "corporateActions": [
    {"ticker":"XXXX","company":"Nama PT","action":"Deskripsi aksi korporasi (20-30 kata)"}
  ],
  "topMovers": {
    "gainers": [{"ticker":"XXXX","changePct":3.5,"why":"Alasan singkat (15-20 kata)"}],
    "losers": [{"ticker":"XXXX","changePct":-2.8,"why":"Alasan singkat"}]
  },
  "watchlist": [{"ticker":"BBCA","why":"Pantau support di XXXX, foreign flow dominan"},{"ticker":"BMRI","why":"..."}],
  "playbook": [
    "Rekomendasi posisi 1 (contoh: Pertimbangkan defensive play di consumer staples)",
    "Rekomendasi posisi 2 (contoh: Pantau rotasi ke sektor perbankan jika asing masuk)",
    "Rekomendasi posisi 3 (contoh: Hedge exposure USD/IDR jika DXY menguat)",
    "Rekomendasi posisi 4"
  ]
}

CATATAN: flow data, topBuys/topSells, corporateActions, topMovers — infer dari headline yang ada. Jika tidak ada data, kosongkan array. Gunakan ticker Indonesia yang relevan.
`.trim();
  } else if (region === "USA") {
    prompt = `
You are writing a US Pre-Market Brief.
Rules: Do NOT invent facts. Based on headlines below. Output valid JSON without markdown.

HEADLINE LIST:
${JSON.stringify(input, null, 2)}

OUTPUT JSON — fill ALL fields using headline data:
{
  "title": "Short title (e.g. S&P Rallies on Strong Earnings)",
  "lede": "Opening paragraph summarizing market conditions (50-80 words, professional tone)",
  "etfFlows": {
    "inflows": [{"symbol":"SPY","flowUsdFmt":"$2.1B","pct":80},{"symbol":"QQQ","flowUsdFmt":"$1.5B","pct":65},{"symbol":"IWM","flowUsdFmt":"$800M","pct":40}],
    "outflows": [{"symbol":"TLT","flowUsdFmt":"-$1.2B","pct":55},{"symbol":"HYG","flowUsdFmt":"-$600M","pct":30},{"symbol":"XLE","flowUsdFmt":"-$400M","pct":20}]
  },
  "watchlist": [{"ticker":"AAPL","why":"Watch earnings reaction at $XXX support"},{"ticker":"NVDA","why":"..."}],
  "playbook": [
    "Action 1 (e.g. Consider rotating into quality tech on pullbacks)",
    "Action 2 (e.g. Monitor 10Y yield for risk-off signals)",
    "Action 3 (e.g. Hedge dollar exposure if DXY breaks above XXX)",
    "Action 4"
  ]
}

NOTE: etfFlows — infer from headlines. Use major ETF tickers. If no data, use empty arrays. pct is relative bar width (0-100).
`.trim();
  } else {
    prompt = `
Kamu menulis Morning Brief pasar untuk region ${region}.
Aturan: Jangan mengarang fakta baru. Berdasarkan headline berikut. Output JSON tanpa markdown.

HEADLINE LIST:
${JSON.stringify(input, null, 2)}

OUTPUT JSON:
{
  "title": "Judul singkat",
  "lede": "Paragraf pembuka 1-2 kalimat (50-80 kata)",
  "playbook": ["3-4 poin watchlist / playbook hari ini"]
}
`.trim();
  }

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
        config: { temperature: 0.35, maxOutputTokens: 2500 }
      });
      const parsed = parseJsonFromText(res.text);
      if (!parsed || typeof parsed !== "object") throw new Error("MorningBrief output invalid");

      const result = {
        asOf,
        title: cleanText(parsed.title || ""),
        lede: cleanText(parsed.lede || ""),
        playbook: Array.isArray(parsed.playbook) ? parsed.playbook.map(x => cleanText(x)).filter(Boolean).slice(0, 6) : [],
      };

      // Indonesia-specific fields
      if (region === "INDONESIA") {
        if (parsed.flow) result.flow = parsed.flow;
        if (parsed.corporateActions) result.corporateActions = parsed.corporateActions;
        if (parsed.topMovers) result.topMovers = parsed.topMovers;
        if (parsed.watchlist) result.watchlist = parsed.watchlist;
      }

      // USA-specific fields
      if (region === "USA") {
        if (parsed.etfFlows) result.etfFlows = parsed.etfFlows;
        if (parsed.watchlist) result.watchlist = parsed.watchlist;
      }

      return result;
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
        imageUrl: it.imageUrl || prev.imageUrl || "",
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
        imageUrl: it.imageUrl || prev.imageUrl || "",
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
  return dedupHeadlines(items).slice().sort((a, b) => {
    const ia = impactRank(a.impact), ib = impactRank(b.impact);
    if (ia !== ib) return ib - ia;
    return (safeDate(b.publishedAt)?.getTime() || 0) - (safeDate(a.publishedAt)?.getTime() || 0);
  }).slice(0, n);
}

/**
 * Deduplicate items by headline similarity.
 * Two items are considered duplicates if:
 *  - Their storyKey matches, OR
 *  - Their normalized headline has >70% word overlap
 */
function dedupHeadlines(items) {
  const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9\s]/gi, "").replace(/\s+/g, " ").trim();
  const wordSet = (s) => new Set(norm(s).split(" ").filter(w => w.length > 2));
  const overlap = (a, b) => {
    if (!a.size || !b.size) return 0;
    let match = 0;
    for (const w of a) { if (b.has(w)) match++; }
    return match / Math.max(a.size, b.size);
  };

  const result = [];
  const seenKeys = new Set();
  const seenWordSets = [];

  for (const it of (items || [])) {
    const key = it.storyKey || it.id;
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);

    const ws = wordSet(it.headline);
    let isDup = false;
    for (const prev of seenWordSets) {
      if (overlap(ws, prev) > 0.7) { isDup = true; break; }
    }
    if (isDup) continue;

    seenWordSets.push(ws);
    result.push(it);
  }
  return result;
}

function buildMorningDeck(region, topItems, indicatorsForRegion, mbText) {
  const tz = REGION_TZ[region] || "UTC";
  const asOf = mbText?.asOf || new Intl.DateTimeFormat("id-ID", { timeZone: tz, dateStyle: "medium", timeStyle: "short" }).format(new Date());

  // CRITICAL: deduplicate items before building any deck
  const dedupedItems = dedupHeadlines(topItems);

  if (region === "INDONESIA") return buildDeckIndonesia(dedupedItems, indicatorsForRegion, mbText, asOf);
  if (region === "USA") return buildDeckUSA(dedupedItems, indicatorsForRegion, mbText, asOf);

  // Default for ASIA/EUROPE (keep original simple layout)
  const title = mbText?.title || `${region} Morning Brief`;
  const lede = mbText?.lede || (dedupedItems.length ? `Tema utama: ${dedupedItems.slice(0, 3).map(x => x.headline).join(" · ")}.` : "");
  const slides = [];

  if (Array.isArray(indicatorsForRegion) && indicatorsForRegion.length) {
    slides.push({ type: "market_snapshot", title: "Market Snapshot", items: indicatorsForRegion.slice(0, 6).map(x => ({ label: x.code, value: x.value, valueFmt: x.valueFmt, changePct: x.changePct, note: x.name || "" })) });
  }
  const impactCards = pickTop(dedupedItems, 3);
  const impactIds = new Set(impactCards.map(x => x.storyKey || x.id));
  if (impactCards.length) {
    slides.push({ type: "impact_cards", title: "Top Drivers", cards: impactCards.map(it => ({ title: it.headline.slice(0, 90), body: (it.keypoints || []).slice(0, 2).join(" ") })) });
  }
  // Exclude items already shown as impact cards
  const newsItems = dedupedItems.filter(x => !impactIds.has(x.storyKey || x.id));
  if (newsItems.length) {
    slides.push({ type: "news_list", title: "Headlines", items: newsItems.slice(0, 5).map(it => ({ headline: it.headline, keypoints: (it.keypoints || []).slice(0, 2) })) });
  }
  const watchlist = extractWatchlist(region, topItems).slice(0, 6).map(t => ({ ticker: t, why: "Pantau level kunci." }));
  const playbook = (mbText?.playbook && mbText.playbook.length) ? mbText.playbook : ["Pantau headline berdampak tinggi.", "Gunakan sizing wajar saat volatilitas naik."];
  slides.push({ type: "watchlist_playbook", watchlist, playbook, disclaimer: "Bukan nasihat keuangan." });
  return { title, asOf, lede, slides };
}

// -------- INDONESIA MORNING DECK --------
function buildDeckIndonesia(topItems, indicators, mbText, asOf) {
  const title = mbText?.title || "Indonesia Morning Briefing";
  const lede = mbText?.lede || "";
  const slides = [];

  // Slide 1: Market Snapshot + Flow IHSG
  // Flow data is embedded in the market_snapshot slide
  const indItems = (indicators || []).slice(0, 7).map(x => ({
    label: x.code, value: x.value, valueFmt: x.valueFmt, changePct: x.changePct, note: x.name || ""
  }));
  const ihsg = indicators?.find(x => x.code === "IHSG" || x.symbol === "^JKSE");
  const ihsgChange = ihsg ? `IHSG ${(ihsg.changePct >= 0 ? "+" : "")}${(ihsg.changePct || 0).toFixed(2)}% ke ${ihsg.valueFmt || ihsg.value || "—"}` : "";

  slides.push({
    type: "market_snapshot",
    title: "Market Snapshot",
    items: indItems,
    flow: {
      foreignBuy: mbText?.flow?.foreignBuy || "—",
      foreignSell: mbText?.flow?.foreignSell || "—",
      domesticBuy: mbText?.flow?.domesticBuy || "—",
      domesticSell: mbText?.flow?.domesticSell || "—",
      foreignNet: mbText?.flow?.foreignNet || { valueFmt: "—" },
      domesticNet: mbText?.flow?.domesticNet || { valueFmt: "—" },
      topBuys: (mbText?.flow?.topBuys || []).slice(0, 3),
      topSells: (mbText?.flow?.topSells || []).slice(0, 3),
      miniNote: ihsgChange || mbText?.flow?.miniNote || "",
    }
  });

  // Slide 2: 3 Impact Events
  const impactCards = pickTop(topItems, 3);
  const impactIds = new Set(impactCards.map(x => x.storyKey || x.id));
  if (impactCards.length) {
    slides.push({
      type: "impact_cards",
      title: "Peristiwa Berdampak Hari Ini",
      cards: impactCards.map(it => ({
        title: it.headline.slice(0, 90),
        body: (it.keypoints || []).slice(0, 2).join(". ").slice(0, 160)
      }))
    });
  }

  // Slide 3: Important Indonesia News (exclude items already in impact cards)
  const newsItems = topItems.filter(x => !impactIds.has(x.storyKey || x.id));
  if (newsItems.length) {
    slides.push({
      type: "news_list",
      title: "Berita Penting Indonesia",
      items: newsItems.slice(0, 5).map(it => ({
        headline: it.headline,
        keypoints: (it.keypoints || []).slice(0, 2),
      }))
    });
  }

  // Slide 4: Corporate Actions + Top Movers LQ45 (merged)
  const corpActions = (mbText?.corporateActions || []).slice(0, 4);
  if (corpActions.length) {
    slides.push({
      type: "corporate_actions",
      title: "Corporate Action",
      items: corpActions.map(c => ({
        ticker: c.ticker || "",
        company: c.company || "",
        action: c.action || c.summary || "",
      }))
    });
  }

  const gainers = (mbText?.topMovers?.gainers || []).slice(0, 3);
  const losers = (mbText?.topMovers?.losers || []).slice(0, 3);
  if (gainers.length || losers.length) {
    slides.push({
      type: "top_movers",
      title: "Top Movers LQ45",
      gainers: gainers.map(g => ({ ticker: g.ticker || "", changePct: g.changePct, why: g.why || "" })),
      losers: losers.map(l => ({ ticker: l.ticker || "", changePct: l.changePct, why: l.why || "" })),
    });
  }

  // Slide 5: Watchlist & Recommendations
  const watchlist = extractWatchlist("INDONESIA", topItems).slice(0, 6).map(t => ({ ticker: t, why: "Pantau reaksi headline & level kunci." }));
  const playbook = (mbText?.playbook && mbText.playbook.length)
    ? mbText.playbook
    : [
      "Pantau data makro global yang berdampak ke sentimen pasar.",
      "Perhatikan rotasi sektoral berdasarkan flow asing.",
      "Gunakan sizing wajar saat volatilitas meningkat.",
    ];

  slides.push({
    type: "watchlist_playbook",
    watchlist: (mbText?.watchlist && mbText.watchlist.length) ? mbText.watchlist : watchlist,
    playbook,
    disclaimer: "Bukan nasihat keuangan."
  });

  return { title, asOf, lede, slides };
}

// -------- USA MORNING DECK --------
function buildDeckUSA(topItems, indicators, mbText, asOf) {
  const title = mbText?.title || "US Pre-Market Brief";
  const lede = mbText?.lede || "";
  const slides = [];

  // Slide 1: Market Snapshot (S&P 500, Nasdaq, Dow, Russell, Bitcoin)
  const wantCodes = ["SPX", "IXIC", "DJI", "RUT", "BTC"];
  const indMap = new Map((indicators || []).map(x => [x.code, x]));
  const snapItems = wantCodes.map(code => {
    const x = indMap.get(code);
    return x ? { label: x.code, value: x.value, valueFmt: x.valueFmt, changePct: x.changePct, note: x.name || "" } : null;
  }).filter(Boolean);
  // Add remaining indicators not in wantCodes
  for (const x of (indicators || [])) {
    if (!wantCodes.includes(x.code) && snapItems.length < 6) {
      snapItems.push({ label: x.code, value: x.value, valueFmt: x.valueFmt, changePct: x.changePct, note: x.name || "" });
    }
  }

  slides.push({
    type: "market_snapshot",
    title: "Market Snapshot",
    items: snapItems,
  });

  // Slide 2: 3 Global Impact Events
  const impactCards = pickTop(topItems, 3);
  const impactIds = new Set(impactCards.map(x => x.storyKey || x.id));
  if (impactCards.length) {
    slides.push({
      type: "impact_cards",
      title: "Global Events Impacting Markets",
      cards: impactCards.map(it => ({
        title: it.headline.slice(0, 90),
        body: (it.keypoints || []).slice(0, 2).join(". ").slice(0, 160)
      }))
    });
  }

  // Slide 3: Corporate & Key News (exclude items already in impact cards)
  const newsItems = topItems.filter(x => !impactIds.has(x.storyKey || x.id));
  if (newsItems.length) {
    slides.push({
      type: "news_list",
      title: "Corporate & Key News",
      items: newsItems.slice(0, 5).map(it => ({
        headline: it.headline,
        keypoints: (it.keypoints || []).slice(0, 2),
      }))
    });
  }

  // Slide 4: Market Conditions (key rates + ETF flows)
  const rateItems = [];
  // Try to extract S&P Futures, 10Y Yield, DXY from indicators
  for (const code of ["VIX", "DXY"]) {
    const x = indMap.get(code);
    if (x) rateItems.push({ label: x.name || x.code, value: x.value, valueFmt: x.valueFmt, note: "" });
  }
  // Add US 10Y and gold if available
  for (const code of ["US10Y", "GOLD", "OIL"]) {
    const x = indMap.get(code);
    if (x && rateItems.length < 4) rateItems.push({ label: x.name || x.code, value: x.value, valueFmt: x.valueFmt, note: "" });
  }

  if (rateItems.length) {
    slides.push({
      type: "macro_rates_fx",
      title: "Key Market Indicators",
      items: rateItems,
    });
  }

  // ETF flows (from mbText if Gemini generated them)
  if (mbText?.etfFlows) {
    slides.push({
      type: "etf_flows",
      title: "ETF Flows",
      inflows: (mbText.etfFlows.inflows || []).slice(0, 3),
      outflows: (mbText.etfFlows.outflows || []).slice(0, 3),
    });
  }

  // Slide 5: Sector Overview (6 cards in 3x2)
  // Use a global set to avoid same headline appearing in multiple sectors
  const usedHeadlines = new Set();
  const sectorCards = [];
  for (const sector of SECTORS) {
    if (sector === "GENERAL") continue;
    const sectorItems = dedupHeadlines(topItems.filter(x =>
      x.sector === sector || (Array.isArray(x.sectors) && x.sectors.includes(sector))
    ));
    const kps = [];
    for (const x of sectorItems) {
      const h = x.headline.slice(0, 80);
      if (usedHeadlines.has(h)) continue;
      usedHeadlines.add(h);
      kps.push(h);
      if (kps.length >= 3) break;
    }
    if (kps.length) {
      sectorCards.push({ sector, keypoints: kps });
    } else {
      sectorCards.push({ sector, keypoints: ["No significant updates in this sector."] });
    }
  }
  if (sectorCards.length) {
    slides.push({ type: "sector_cards", title: "Sector Overview", items: sectorCards });
  }

  // Slide 6: Watchlist & Recommendations
  const watchlist = extractWatchlist("USA", topItems).slice(0, 6).map(t => ({ ticker: t, why: "Watch for headline reaction & key levels." }));
  const playbook = (mbText?.playbook && mbText.playbook.length)
    ? mbText.playbook
    : [
      "Monitor macro data releases and Fed commentary for risk sentiment shifts.",
      "Focus on high-impact news; ignore daily noise.",
      "Size positions appropriately during elevated volatility.",
    ];

  slides.push({
    type: "watchlist_playbook",
    watchlist: (mbText?.watchlist && mbText.watchlist.length) ? mbText.watchlist : watchlist,
    playbook,
    disclaimer: "Not financial advice."
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
    const allBaseItems = rssItems
      .map(x => makeBaseItem({
        region,
        headline: x.title,
        link: x.link,
        publishedAt: safeDate(x.pubDate)?.toISOString() || nowIso(),
        description: x.description,
        sourceName: x.sourceName || "Source",
        sourceUrl: x.sourceUrl || x.link,
        articleUrl: x.articleUrl || "",
        hintSector: x.hintSector,
      }));
    const afterTrust = allBaseItems.filter(isTrustedItem);
    const afterRelevance = afterTrust.filter(x => isMarketRelevant(x.headline, x.story || ""));
    const freshBase = afterRelevance.filter(x => withinHours(x.publishedAt, TTL_HOURS, now));
    console.log(`[worker] region=${region} pipeline: rss=${rssItems.length} → base=${allBaseItems.length} → trust=${afterTrust.length} → relevant=${afterRelevance.length} → ttl=${freshBase.length}`);

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

    // Fetch article content for items that need rewriting
    let articleTexts = [];
    if (toRewrite.length) {
      console.log(`[worker] region=${region} fetching article content for ${toRewrite.length} items...`);
      articleTexts = await enrichItemsWithArticleContent(toRewrite);
      const fetchedCount = articleTexts.filter(t => t.length > 0).length;
      console.log(`[worker] region=${region} fetched article content: ${fetchedCount}/${toRewrite.length} succeeded`);
    }

    let rewritten = toRewrite;
    if (toRewrite.length && ai) {
      console.log(`[worker] region=${region} gemini rewrite count=${toRewrite.length}`);
      rewritten = await rewriteItemsWithGemini(region, toRewrite, articleTexts);
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
    const releaseMinute = MORNING_RELEASE_MINUTE[region] || 0;
    const todayKey = localDateKey(region, startedAt);

    // Check if today is a weekday (Mon-Fri) in the region's timezone
    const tz = REGION_TZ[region] || "UTC";
    const dayOfWeek = new Intl.DateTimeFormat("en-US", { timeZone: tz, weekday: "short" }).format(startedAt);
    const isWeekday = !["Sat", "Sun"].includes(dayOfWeek);

    const shouldGenerate = isWeekday && (
      (RUN_MODE === "morning") ||
      (RUN_MODE === "full") ||
      (hour > releaseHour || (hour === releaseHour && minute >= releaseMinute))
    );

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
