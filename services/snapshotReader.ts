import { Region, Sector, MarketSnapshot } from "../types";

export const getLatestSnapshot = async (
  region: Region, 
  sector: Sector,
  onFreshData?: (data: MarketSnapshot) => void
): Promise<MarketSnapshot | null> => {
  
  // Use specific cache key to prevent data bleeding between regions/sectors
  const CACHE_KEY = `VELRA_V4_${region}_${sector}`;
  
  // Network-First Strategy
  try {
    const params = new URLSearchParams({
      region: region,
      sector: sector,
      cb: Date.now().toString() // Cache busting
    });

    const response = await fetch(`/api/snapshot?${params.toString()}`, {
       cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const freshData: MarketSnapshot = await response.json();
    
    // Update Cache & State
    try {
      localStorage.setItem(CACHE_KEY, JSON.stringify(freshData));
    } catch (e) {
      console.warn("LocalStorage write failed (quota?)", e);
    }

    if (onFreshData) onFreshData(freshData);
    return freshData;

  } catch (e: any) {
    console.warn(`[Reader] Network failed for ${region}/${sector}, trying cache...`, e.message);
    
    // Fallback to Cache
    try {
      const cached = localStorage.getItem(CACHE_KEY);
      if (cached) {
        const data = JSON.parse(cached);
        if (onFreshData) onFreshData(data);
        return data;
      }
    } catch (cacheErr) { 
      console.warn("Cache read fail", cacheErr); 
    }
    
    return null;
  }
};