
import { Storage } from '@google-cloud/storage';
import { Region, Sector, MarketSnapshot } from '../types';

const storage = new Storage();
const BUCKET_NAME = process.env.GCS_BUCKET_NAME || 'velra-market-snapshots';

const REGION_CODE_MAP: Record<string, string> = {
  [Region.INDONESIA]: 'ID',
  [Region.US]: 'US',
  [Region.ASIA]: 'ASIA',
  [Region.EUROPE]: 'EU',
  [Region.AMERICAS]: 'AM'
};

const SECTOR_CODE_MAP: Record<string, string> = {
  [Sector.ALL]: 'ALL',
  [Sector.TECHNOLOGY]: 'TECH',
  [Sector.FINANCE]: 'FIN',
  [Sector.MINING]: 'MINING',
  [Sector.HEALTHCARE]: 'HEALTH',
  [Sector.REGULATION]: 'REG',
  [Sector.CONSUMER]: 'CONS'
};

const getFileName = (region: Region, sector: Sector) => {
  const r = REGION_CODE_MAP[region] || 'ID';
  const s = SECTOR_CODE_MAP[sector] || 'ALL';
  return `latest_${r}_${s}.json`;
};

export const checkFileExists = async (region: Region, sector: Sector): Promise<boolean> => {
  try {
    const bucket = storage.bucket(BUCKET_NAME);
    const file = bucket.file(getFileName(region, sector));
    const [exists] = await file.exists();
    return exists;
  } catch (e) {
    return false;
  }
};

export const uploadSnapshot = async (region: Region, sector: Sector, data: MarketSnapshot) => {
  if (!data || !data.metadata) throw new Error("Invalid Snapshot Data");

  const bucket = storage.bucket(BUCKET_NAME);
  const fileName = getFileName(region, sector);
  const file = bucket.file(fileName);
  
  // STRICT NO-CACHE: The edge servers (CDN) and browsers must NOT cache this file.
  // We want realtime updates.
  await file.save(JSON.stringify(data), {
    contentType: 'application/json',
    metadata: {
      cacheControl: 'public, max-age=0, no-cache, no-store, must-revalidate', 
    }
  });

  // Archive history (Async, fire and forget)
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const histName = `history/${REGION_CODE_MAP[region]}/${SECTOR_CODE_MAP[sector]}/${ts}.json`;
  bucket.file(histName).save(JSON.stringify(data), { contentType: 'application/json' }).catch(console.error);

  return `https://storage.googleapis.com/${BUCKET_NAME}/${fileName}`;
};

export const downloadSnapshot = async (region: Region, sector: Sector): Promise<MarketSnapshot | null> => {
  const bucket = storage.bucket(BUCKET_NAME);
  const file = bucket.file(getFileName(region, sector));

  try {
    const [exists] = await file.exists();
    if (!exists) return null;
    
    const [content] = await file.download();
    return JSON.parse(content.toString());
  } catch (e) {
    console.error(`[Storage] Download failed: ${getFileName(region, sector)}`, e);
    return null;
  }
};
