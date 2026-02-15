import { GoogleGenAI } from "@google/genai";
import { Region, Sector, MarketSnapshot, NewsItem, SmartOutlookContent } from '../types.js';

// Fix: Robust API Key check
const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || process.env.API_KEY;
if (!apiKey) {
  console.error("CRITICAL: No API Key found in env (checked GEMINI_API_KEY, GOOGLE_API_KEY, API_KEY)");
}
const ai = new GoogleGenAI({ apiKey: apiKey || 'dummy-key-to-prevent-crash-until-call' });

// --- CONFIG ---
const TRUSTED_DOMAINS = [
  'cnbc.com', 'cnbcindonesia.com', 'bloomberg.com', 'reuters.com', 
  'bisnis.com', 'kontan.co.id', 'investing.com', 'marketwatch.com', 
  'finance.yahoo.com', 'wsj.com', 'ft.com', 'cnn.com', 'kompas.com', 
  'investor.id', 'emitennews.com'
];

// --- UTILS ---
const isValidNewsUrl = (urlStr: string): boolean => {
  try {
    const url = new URL(urlStr);
    if (!['http:', 'https:'].includes(url.protocol)) return false;
    return TRUSTED_DOMAINS.some(d => url.hostname.endsWith(d));
  } catch (e) { return false; }
};

const getNewsTimeWindow = () => {
  // Fix: Force WIB (UTC+7) regardless of Server Time
  const now = new Date();
  const options: Intl.DateTimeFormatOptions = { 
    timeZone: "Asia/Jakarta", 
    hour: 'numeric', 
    hour12: false 
  };
  const formatter = new Intl.DateTimeFormat('en-US', options);
  const currentHourStr = formatter.format(now);
  const currentHour = parseInt(currentHourStr, 10);
  
  const prevHour = (currentHour - 1 + 24) % 24; 
  const pad = (n: number) => String(n).padStart(2, '0');
  
  return `${pad(prevHour)}:00 - ${pad(currentHour)}:00`;
};

const parseJson = (text: string | undefined): any => {
  if (!text) return null;
  try {
    const cleanText = text.replace(/```json|```/g, '').trim();
    const startIndex = cleanText.indexOf('{');
    const arrayStart = cleanText.indexOf('[');
    if (arrayStart !== -1 && (startIndex === -1 || arrayStart < startIndex)) {
      return JSON.parse(cleanText.substring(arrayStart, cleanText.lastIndexOf(']') + 1));
    }
    return JSON.parse(cleanText.substring(startIndex, cleanText.lastIndexOf('}') + 1));
  } catch (e) { return null; }
};

const extractSources = (response: any) => {
  const sources: any[] = [];
  const chunks = response.candidates?.[0]?.groundingMetadata?.groundingChunks;
  if (chunks) {
    chunks.forEach((chunk: any) => {
      if (chunk.web) sources.push({ title: chunk.web.title || 'Source', uri: chunk.web.uri });
    });
  }
  return sources;
};

// --- GENERATORS ---
async function genNews(region: Region, sector: Sector) {
  const window = getNewsTimeWindow();
  const prompt = `
    ROLE: Strict Market News Validator.
    REGION: ${region} (${sector}).
    TIME WINDOW: ${window} WIB Today.
    TASK: Get latest market news. Include 'sourceUrl'. Filter old news (>24h).
    OUTPUT JSON ARRAY: [{headline, impact (High/Medium), time (HH:MM), summaryPoints [], sourceName, sourceUrl}]
  `;
  try {
    const res = await ai.models.generateContent({
        model: "gemini-3-flash-preview", 
        contents: prompt, 
        config: { tools: [{ googleSearch: {} }] }
    });
    let items: NewsItem[] = parseJson(res.text) || [];
    items = items.filter(item => item.sourceUrl && isValidNewsUrl(item.sourceUrl));
    return { items, sources: extractSources(res) };
  } catch (e) { return { items: [], sources: [] }; }
}

async function genOutlook(region: Region, sector: Sector) {
  const isMorning = sector === Sector.ALL;
  const dateStr = new Date().toLocaleDateString('id-ID', { timeZone: "Asia/Jakarta" });
  const prompt = `
    ROLE: Portfolio Manager. DATE: ${dateStr}.
    TASK: ${isMorning ? 'Morning Briefing' : 'Sector Intelligence'} for ${region} (${sector}).
    OUTPUT JSON: {
      type: "${isMorning ? 'BRIEFING' : 'OUTLOOK'}",
      headline: "Title",
      marketOverview: "Summary",
      topNews: [{headline, description}],
      watchlist: [{ticker, note}],
      recommendation: {verdict, analysis},
      date: "${dateStr}"
    }
  `;
  try {
    const res = await ai.models.generateContent({
        model: "gemini-3-flash-preview", 
        contents: prompt, 
        config: { tools: [{ googleSearch: {} }] }
    });
    const parsed = parseJson(res.text);
    // Safe structure construction
    const safeData: SmartOutlookContent | null = parsed ? {
        type: parsed.type || (isMorning ? 'BRIEFING' : 'OUTLOOK'),
        date: parsed.date || dateStr,
        headline: parsed.headline || 'Market Outlook',
        marketOverview: parsed.marketOverview || 'Data unavailable.',
        topNews: Array.isArray(parsed.topNews) ? parsed.topNews : [],
        watchlist: Array.isArray(parsed.watchlist) ? parsed.watchlist : [],
        recommendation: parsed.recommendation || { verdict: 'NEUTRAL', analysis: 'Wait for data.' },
        sectorUpdates: parsed.sectorUpdates,
        foreignFlow: parsed.foreignFlow,
        usBriefing: parsed.usBriefing,
        corporateActions: parsed.corporateActions
    } : null;
    return { data: safeData, sources: extractSources(res) };
  } catch (e) { return { data: null, sources: [] }; }
}

async function genIndices(region: Region) {
   const prompt = `Get Real-time indices for ${region}. Output JSON [{symbol, name, price, change, percentChange, trend}]`;
   try {
    const res = await ai.models.generateContent({
        model: "gemini-3-flash-preview", 
        contents: prompt, 
        config: { tools: [{ googleSearch: {} }] }
    });
    return { indices: parseJson(res.text) || [], sources: extractSources(res) };
  } catch (e) { return { indices: [], sources: [] }; }
}

async function genEcon(region: Region) {
   if (![Region.INDONESIA, Region.US].includes(region)) return { items: [], sources: [] };
   const prompt = `Get latest economic data for ${region}. Output JSON [{metric, actual, previous, forecast, nextRelease, history}]`;
   try {
    const res = await ai.models.generateContent({
        model: "gemini-3-flash-preview", 
        contents: prompt, 
        config: { tools: [{ googleSearch: {} }] }
    });
    return { items: parseJson(res.text) || [], sources: extractSources(res) };
  } catch (e) { return { items: [], sources: [] }; }
}

export const generateMarketSnapshot = async (region: Region, sector: Sector): Promise<MarketSnapshot> => {
  const [news, outlook, indices, econ] = await Promise.all([
    genNews(region, sector),
    genOutlook(region, sector),
    genIndices(region),
    genEcon(region)
  ]);

  const timestamp = new Date().toISOString();

  // STRICT SCHEMA CONSTRUCTION
  return {
    metadata: {
      generatedAt: timestamp,
      region: region, 
      sector: sector, 
      // Important: Use 'writer-' + timestamp so frontend knows it's a fresh server build
      versionId: `writer-${Date.now()}`
    },
    outlook: outlook,
    news: news,
    indices: indices,
    economics: econ 
  };
};