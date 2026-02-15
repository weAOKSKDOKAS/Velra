import React, { useState, useEffect, useMemo } from 'react';
import Ticker from './components/Ticker';
import Sidebar from './components/Sidebar';
import NewsItemCard from './components/NewsItemCard';
import EconomicTable from './components/EconomicTable';
import BriefingModal from './components/BriefingModal';
import NewsDetailModal from './components/NewsDetailModal';
import MarketDashboard from './components/MarketChart'; 
import { Region, Sector, MarketSnapshot, NewsItem, NewsArticle } from './types';
import { getLatestSnapshot } from './services/snapshotReader';
import { fetchNewsDetail } from './services/geminiService'; 

const Skeleton = () => (
  <div className="animate-pulse space-y-4">
    <div className="h-4 bg-white/5 rounded w-3/4"></div>
    <div className="h-24 bg-white/5 rounded-xl w-full"></div>
  </div>
);

const App: React.FC = () => {
  const [activeRegion, setActiveRegion] = useState<Region>(Region.INDONESIA);
  const [activeSector, setActiveSector] = useState<Sector>(Sector.ALL);
  const [sortBy, setSortBy] = useState<'LATEST' | 'IMPACT'>('LATEST');
  
  const [snapshot, setSnapshot] = useState<MarketSnapshot | null>(null);
  const [isInitializing, setIsInitializing] = useState<boolean>(true); 
  const [isSyncing, setIsSyncing] = useState<boolean>(false); 
  const [errorDetails, setErrorDetails] = useState<string | null>(null);
  
  const [isBriefingOpen, setIsBriefingOpen] = useState(false);
  const [selectedNews, setSelectedNews] = useState<NewsItem | null>(null);
  const [newsDetail, setNewsDetail] = useState<NewsArticle | null>(null);
  const [loadingNewsDetail, setLoadingNewsDetail] = useState(false);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  const loadData = async () => {
    setIsSyncing(true);
    if (!snapshot) setIsInitializing(true);
    setErrorDetails(null);

    try {
      const data = await getLatestSnapshot(activeRegion, activeSector, (fresh) => {
           setSnapshot(fresh);
           setIsSyncing(false);
      });
      
      if (data) {
        setSnapshot(data);
      } else {
        setErrorDetails("Endpoint /data.json returned an empty or invalid response.");
      }
    } catch (error: any) {
      setErrorDetails(`Connection failed: ${error.message}`);
    } finally {
      setIsInitializing(false);
      setTimeout(() => setIsSyncing(false), 1500);
    }
  };

  useEffect(() => { loadData(); }, [activeRegion, activeSector]);

  useEffect(() => {
    const interval = setInterval(() => loadData(), 60000); 
    return () => clearInterval(interval);
  }, [activeRegion, activeSector]);

  const sortedNewsItems = useMemo(() => {
    if (!snapshot?.news?.items) return [];
    let items = [...snapshot.news.items];
    if (sortBy === 'IMPACT') {
      const score = { 'High': 3, 'Medium': 2, 'Low': 1 };
      items.sort((a, b) => (score[b.impact] as any || 0) - (score[a.impact] as any || 0));
    } else {
      items.sort((a, b) => (b.time || "").localeCompare(a.time || ""));
    }
    return items;
  }, [snapshot, sortBy]);

  const handleOpenNews = async (item: NewsItem) => {
    setSelectedNews(item); 
    setNewsDetail(null); 
    setLoadingNewsDetail(true);
    const article = await fetchNewsDetail(item.headline, item.sourceUrl);
    setNewsDetail(article); 
    setLoadingNewsDetail(false);
  };

  return (
    <div className="min-h-screen bg-[#050505] text-white flex flex-col font-sans">
      <Ticker newsItems={snapshot?.news?.items || []} loading={isInitializing} />
      
      <div className="flex flex-1 overflow-hidden">
        <Sidebar 
          activeRegion={activeRegion} 
          setActiveRegion={setActiveRegion} 
          activeSector={activeSector} 
          setActiveSector={setActiveSector} 
          isOpen={isMobileMenuOpen} 
          onClose={() => setIsMobileMenuOpen(false)} 
        />

        <main className="flex-1 p-4 md:p-8 overflow-y-auto custom-scrollbar bg-gradient-to-b from-[#080808] to-black">
          <div className="max-w-[1400px] mx-auto space-y-10">
            
            {!snapshot && !isInitializing && (
               <div className="border border-red-500/20 bg-red-500/5 p-12 rounded-xl text-center space-y-6 animate-fade-in">
                  <div className="w-16 h-16 bg-red-500/10 rounded-full flex items-center justify-center mx-auto border border-red-500/20">
                     <svg className="w-8 h-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>
                  </div>
                  <div>
                    <p className="text-red-500 font-black tracking-widest uppercase text-xl">MATRIX DISCONNECTED</p>
                    <p className="text-gray-500 text-sm font-mono mt-2 max-w-md mx-auto p-4 bg-black/50 border border-white/5 rounded">
                      DIAGNOSTICS: {errorDetails}
                    </p>
                  </div>
                  <button onClick={() => loadData()} className="px-8 py-3 bg-white text-black text-xs font-black uppercase hover:bg-[#d4af37] transition-all rounded shadow-xl active:scale-95">
                    Retry Connection Now
                  </button>
               </div>
            )}

            {snapshot && (
              <>
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                  <div className="lg:col-span-2 space-y-8">
                    <div 
                      className="glass-panel rounded-xl border border-white/10 p-10 relative group cursor-pointer overflow-hidden transition-all hover:border-[#d4af37]/40 shadow-2xl" 
                      onClick={() => setIsBriefingOpen(true)}
                    >
                      <div className="absolute top-0 right-0 w-32 h-32 bg-[#d4af37]/5 blur-3xl rounded-full"></div>
                      <div className="relative z-10">
                        <div className="flex items-center gap-3 mb-6">
                           <span className="text-[10px] font-black text-[#d4af37] bg-[#d4af37]/10 px-3 py-1 rounded uppercase tracking-[0.2em] border border-[#d4af37]/20">Macro Intel</span>
                           {isSyncing && <span className="text-[9px] text-green-500 font-mono animate-pulse uppercase tracking-widest">Syncing...</span>}
                        </div>
                        <h2 className="text-4xl md:text-5xl font-black italic tracking-tighter font-serif mb-6 leading-none">
                           {snapshot?.outlook?.data?.headline || 'Market Intelligence Stream'}
                        </h2>
                        <p className="text-gray-400 text-base leading-relaxed line-clamp-3 font-light">
                           {snapshot?.outlook?.data?.marketOverview || 'Initializing overview data...'}
                        </p>
                        <div className="mt-8 flex items-center gap-2 text-[#d4af37] text-[11px] font-black uppercase tracking-[0.2em] group-hover:gap-4 transition-all">
                          Open Analysis Playbook <span className="text-lg">→</span>
                        </div>
                      </div>
                    </div>
                    <MarketDashboard indices={snapshot?.indices?.indices || []} loading={isInitializing} region={activeRegion} />
                  </div>

                  <div className="glass-panel rounded-xl p-8 border border-white/10 shadow-xl bg-black/40">
                    <h2 className="text-[10px] font-black text-gray-500 uppercase tracking-[0.3em] mb-8 flex items-center gap-2">
                       <span className="w-2 h-2 bg-[#d4af37] rounded-full"></span>
                       Key Indicators
                    </h2>
                    {isInitializing ? <Skeleton /> : <EconomicTable data={snapshot?.economics?.items || []} />}
                  </div>
                </div>

                <div className="space-y-8 pt-4">
                  <div className="flex justify-between items-end border-b border-white/10 pb-6">
                      <div>
                        <h2 className="text-3xl font-black text-white italic font-serif tracking-tight">Wire: {activeRegion}</h2>
                        <p className="text-[10px] text-gray-500 font-mono mt-2 uppercase tracking-widest">Feed Status: <span className="text-green-500">Live</span> // Sector: {activeSector}</p>
                      </div>
                      <div className="flex bg-[#111] rounded-lg p-1 border border-white/5">
                        <button onClick={() => setSortBy('LATEST')} className={`px-5 py-2 text-[10px] font-black tracking-widest rounded-md transition-all ${sortBy === 'LATEST' ? 'bg-[#d4af37] text-black' : 'text-gray-500 hover:text-white'}`}>LATEST</button>
                        <button onClick={() => setSortBy('IMPACT')} className={`px-5 py-2 text-[10px] font-black tracking-widest rounded-md transition-all ${sortBy === 'IMPACT' ? 'bg-[#d4af37] text-black' : 'text-gray-500 hover:text-white'}`}>IMPACT</button>
                      </div>
                  </div>
                  
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                    {sortedNewsItems.map((item, idx) => (
                        <NewsItemCard key={idx} item={item} onReadMore={() => handleOpenNews(item)} />
                    ))}
                  </div>
                </div>
              </>
            )}
          </div>
        </main>
      </div>

      <BriefingModal isOpen={isBriefingOpen} onClose={() => setIsBriefingOpen(false)} data={snapshot?.outlook?.data || null} sources={snapshot?.outlook?.sources || []} region={activeRegion} onSectorSelect={(s) => {setActiveSector(s); setIsBriefingOpen(false);}} />
      <NewsDetailModal isOpen={!!selectedNews} onClose={() => setSelectedNews(null)} article={newsDetail} loading={loadingNewsDetail} newsItem={selectedNews} />
      
      <button 
        onClick={() => setIsMobileMenuOpen(true)} 
        className="md:hidden fixed bottom-8 right-8 w-14 h-14 bg-[#d4af37] text-black rounded-full shadow-2xl flex items-center justify-center z-[70] hover:scale-110 active:scale-95 transition-all"
      >
         <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16m-7 6h7" /></svg>
      </button>
    </div>
  );
};

export default App;
