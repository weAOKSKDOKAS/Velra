

export enum Region {
  INDONESIA = 'Indonesia',
  US = 'Amerika Serikat',
  ASIA = 'Asia',
  EUROPE = 'Eropa'
}

export enum Sector {
  ALL = 'Umum',
  TECHNOLOGY = 'Teknologi',
  FINANCE = 'Keuangan',
  MINING = 'Energi & Tambang',
  HEALTHCARE = 'Kesehatan',
  REGULATION = 'Regulasi',
  CONSUMER = 'Barang Konsumsi'
}

export interface GroundingSource {
  title: string;
  uri: string;
}

export interface NewsItem {
  headline: string;
  impact: 'High' | 'Medium' | 'Low';
  time: string; // HH:MM
  summaryPoints: string[];
  sourceName: string;
  sourceUrl: string;
}

export interface NewsArticle {
  headline: string;
  author: string;
  timestamp: string;
  takeaways: string[];
  content: string; 
  sources?: GroundingSource[];
}

export interface MarketIndex {
  symbol: string;
  name: string;
  price: string;
  change: string;
  percentChange: string;
  trend: 'UP' | 'DOWN' | 'FLAT';
}

export interface EconomicIndicator {
  metric: string;
  previous: string;
  actual: string;
  forecast: string;
  nextRelease: string;
  history: number[]; 
}

// --- OUTLOOK / BRIEFING TYPES ---

export interface TopNewsItem {
  headline: string;
  description: string;
}

export interface SectorUpdate {
  overview: string;
}

export interface EtfFlowItem {
  ticker: string;
  name: string;
  amount: number;
}

export interface USHeatmapItem {
  sector: string;
  change: string;
  trend: 'UP' | 'DOWN' | 'FLAT';
}

export interface USBriefingData {
  topNews: TopNewsItem[];
  snapshot: {
    indices: { name: string; value: string; change: string }[];
    macro: {
      us10y: string;
      dxy: string;
      vix: string;
    };
  };
  etfFlows: {
    creation: EtfFlowItem[];
    redemption: EtfFlowItem[];
  }; 
  sectorHeatmap: USHeatmapItem[];
  mixedNews: {
    headline: string;
    category: 'Corporate' | 'Politics' | 'Macro';
    impact: 'High' | 'Medium';
  }[];
  watchlist: {
    ticker: string;
    reason: string;
  }[];
}

export interface ForeignFlowData {
  netStatus: string; 
  topBuy: { ticker: string; value: string }[];
  topSell: { ticker: string; value: string }[];
}

export interface SmartOutlookContent {
  type: 'BRIEFING' | 'OUTLOOK'; 
  date: string; 
  headline?: string;
  marketOverview: string; 
  topNews: TopNewsItem[];
  sectorUpdates?: {
    [key in Sector]?: SectorUpdate;
  };
  foreignFlow?: ForeignFlowData;
  usBriefing?: USBriefingData;
  corporateActions?: {
    ticker: string;
    action: string;
    date?: string;
  }[];
  watchlist: { ticker: string; note: string }[];
  recommendation: {
    verdict: string; 
    analysis: string;
  };
}

// --- SNAPSHOT ARCHITECTURE ---

export interface MarketSnapshot {
  metadata: {
    generatedAt: string; // ISO String from Server
    region: Region;
    sector: Sector;
    versionId: string; // UUID or Hash
  };
  outlook: {
    data: SmartOutlookContent | null;
    sources: GroundingSource[];
  };
  news: {
    items: NewsItem[];
    sources: GroundingSource[];
  };
  indices: {
    indices: MarketIndex[];
    sources: GroundingSource[];
  };
  economics: {
    items: EconomicIndicator[];
    sources: GroundingSource[];
  };
}

export interface MarketUpdate {
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  timestamp: string;
}
