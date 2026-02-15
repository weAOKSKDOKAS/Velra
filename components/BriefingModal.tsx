
import React from 'react';
import { SmartOutlookContent, GroundingSource, Region, Sector, SectorUpdate } from '../types';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ReferenceLine, LabelList } from 'recharts';

interface BriefingModalProps {
  isOpen: boolean;
  onClose: () => void;
  data: SmartOutlookContent | null;
  sources: GroundingSource[];
  region: Region;
  onSectorSelect: (sector: Sector) => void;
}

const BriefingModal: React.FC<BriefingModalProps> = ({ isOpen, onClose, data, sources, region, onSectorSelect }) => {
  if (!isOpen) return null;

  const sectorMap: Record<string, Sector> = {
    'Teknologi': Sector.TECHNOLOGY, 'Technology': Sector.TECHNOLOGY,
    'Keuangan': Sector.FINANCE, 'Finance': Sector.FINANCE,
    'Energi & Tambang': Sector.MINING, 'Energi': Sector.MINING, 'Energy': Sector.MINING,
    'Kesehatan': Sector.HEALTHCARE, 'Healthcare': Sector.HEALTHCARE,
    'Regulasi': Sector.REGULATION, 'Regulation': Sector.REGULATION,
    'Barang Konsumsi': Sector.CONSUMER, 'Consumer': Sector.CONSUMER,
    'Umum': Sector.ALL
  };

  const isMorningBriefing = data?.type === 'BRIEFING';
  const isUSBriefing = data?.usBriefing && region === Region.US;

  // Prepare ETF Flow Data for Chart (Horizontal Bar)
  const etfChartData = isUSBriefing && data.usBriefing?.etfFlows ? [
      ...data.usBriefing.etfFlows.redemption.map(i => ({ name: i.ticker, value: -Math.abs(i.amount), type: 'Redemption' })),
      ...data.usBriefing.etfFlows.creation.map(i => ({ name: i.ticker, value: i.amount, type: 'Creation' }))
  ].sort((a,b) => b.value - a.value) : [];

  const title = isUSBriefing ? "US MARKET BRIEF" : (isMorningBriefing ? "MARKET BRIEFING" : "SECTOR INTELLIGENCE");

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-0 md:p-6 lg:p-8">
      <div 
        className="absolute inset-0 bg-[#000000]/90 backdrop-blur-2xl animate-fade-in transition-opacity duration-300"
        onClick={onClose}
      ></div>

      <div className="relative w-full max-w-[1400px] h-full bg-[#080808] border border-white/10 shadow-2xl flex flex-col overflow-hidden animate-fade-in md:rounded-xl ring-1 ring-white/5">
        
        {/* HEADER */}
        <div className="flex justify-between items-end p-8 border-b border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md sticky top-0 z-20 shrink-0">
          <div>
            <div className="flex items-center gap-2 mb-2">
                <span className="w-2 h-2 bg-accent rounded-full animate-pulse shadow-[0_0_10px_rgba(212,175,55,0.5)]"></span>
                <span className="text-accent text-[10px] font-mono uppercase tracking-[0.2em]">VELRA {isMorningBriefing ? 'MACRO' : 'SECTOR'}</span>
            </div>
            <h2 className="text-3xl md:text-5xl font-black text-white tracking-tighter uppercase font-serif italic">
              {title}<span className="text-accent">.</span>
            </h2>
            <p className="text-secondary font-mono text-sm mt-2 flex items-center gap-2">
               <span className="opacity-50">PUBLISHED:</span> {data?.date}
            </p>
          </div>
          <button onClick={onClose} className="w-10 h-10 rounded-full border border-white/10 flex items-center justify-center text-secondary hover:text-white hover:border-white hover:bg-white/5 transition-all">
             <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
           </button>
        </div>

        {/* CONTENT */}
        <div className="flex-1 overflow-y-auto custom-scrollbar bg-[#050505] p-6 md:p-12 relative">
          <div className="absolute top-0 left-1/2 -translate-x-1/2 w-[80%] h-[300px] bg-accent/5 blur-[120px] pointer-events-none rounded-full"></div>

          {!data ? (
             <div className="flex items-center justify-center h-full flex-col gap-6">
               <div className="relative w-16 h-16"><div className="absolute inset-0 border-t-2 border-accent rounded-full animate-spin"></div></div>
               <div className="animate-pulse text-secondary font-mono text-xs tracking-[0.2em] uppercase">GENERATING INTELLIGENCE...</div>
             </div>
          ) : (
            <div className="max-w-[1200px] mx-auto space-y-12 relative z-10">

              {/* ======================= US SPECIFIC BRIEFING ======================= */}
              {isUSBriefing && data.usBriefing ? (
                 <>
                   {/* 01. TOP NEWS (Moved to Top) */}
                   <section>
                      <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6 font-sans">01 // TOP NEWS</h3>
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                         {(data.usBriefing.topNews || []).slice(0,3).map((news, i) => (
                           <div key={i} className="bg-[#0A0A0A] border border-white/10 p-6 hover:border-accent/30 transition-colors group">
                              <h4 className="text-white font-bold text-lg mb-3 font-serif leading-tight group-hover:text-accent transition-colors">{news.headline}</h4>
                              <p className="text-sm text-gray-400 leading-relaxed font-sans">{news.description}</p>
                           </div>
                         ))}
                      </div>
                   </section>

                   {/* 02. SNAPSHOT (Split View) */}
                   <section className="border-y border-white/5 py-12">
                      <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
                         {/* Left: Indices Cards */}
                         <div>
                            <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6">02 // INDICES SNAPSHOT</h3>
                            <div className="grid grid-cols-2 gap-4">
                               {data.usBriefing.snapshot.indices.map((idx, i) => (
                                  <div key={i} className="bg-[#0A0A0A] border border-white/10 p-4 flex flex-col justify-between h-[100px] hover:bg-white/5 transition-colors">
                                     <div className="flex justify-between items-start">
                                        <span className="text-xs font-bold text-white font-mono">{idx.name}</span>
                                        <span className={`text-xs font-bold font-mono ${idx.change.includes('+') ? 'text-success' : 'text-danger'}`}>{idx.change}</span>
                                     </div>
                                     <div className="text-xl font-black text-white font-serif">{idx.value}</div>
                                  </div>
                               ))}
                            </div>
                         </div>
                         {/* Right: Macro Conditions */}
                         <div>
                             <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6">03 // MACRO CONDITIONS</h3>
                             <div className="grid grid-cols-3 gap-4 h-full max-h-[220px]">
                                <div className="border border-white/10 p-6 flex flex-col items-center justify-center text-center">
                                   <span className="text-[10px] text-secondary font-mono mb-2 uppercase">US 10Y Yield</span>
                                   <span className="text-3xl font-black text-white">{data.usBriefing.snapshot.macro.us10y}</span>
                                </div>
                                <div className="border border-white/10 p-6 flex flex-col items-center justify-center text-center">
                                   <span className="text-[10px] text-secondary font-mono mb-2 uppercase">DXY Index</span>
                                   <span className="text-3xl font-black text-accent">{data.usBriefing.snapshot.macro.dxy}</span>
                                </div>
                                <div className="border border-white/10 p-6 flex flex-col items-center justify-center text-center">
                                   <span className="text-[10px] text-secondary font-mono mb-2 uppercase">VIX (Fear)</span>
                                   <span className={`text-3xl font-black ${parseFloat(data.usBriefing.snapshot.macro.vix) > 20 ? 'text-danger' : 'text-success'}`}>
                                     {data.usBriefing.snapshot.macro.vix}
                                   </span>
                                </div>
                             </div>
                         </div>
                      </div>
                   </section>

                   {/* 04. FLOW ANALYSIS (Chart) */}
                   <section>
                      <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6">04 // ETF FLOW ANALYSIS</h3>
                      <div className="bg-[#0A0A0A] border border-white/10 p-6 h-[300px] w-full">
                         <ResponsiveContainer width="100%" height="100%">
                            <BarChart layout="vertical" data={etfChartData} margin={{top: 5, right: 30, left: 40, bottom: 5}}>
                               <XAxis type="number" hide />
                               <YAxis dataKey="name" type="category" width={80} tick={{fill: '#9ca3af', fontSize: 10}} />
                               <Tooltip 
                                  contentStyle={{backgroundColor: '#000', borderColor: '#333', color: '#fff'}}
                                  itemStyle={{color: '#fff'}}
                                  cursor={{fill: 'rgba(255,255,255,0.05)'}}
                               />
                               <ReferenceLine x={0} stroke="#333" />
                               <Bar dataKey="value" name="Flow ($M)">
                                  {etfChartData.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={entry.value > 0 ? '#4ade80' : '#f87171'} />
                                  ))}
                                  <LabelList dataKey="value" position={d => d.value > 0 ? 'right' : 'left'} style={{fill: '#fff', fontSize: '10px'}} />
                               </Bar>
                            </BarChart>
                         </ResponsiveContainer>
                      </div>
                   </section>

                   {/* 05. SECTOR HEATMAP */}
                   <section>
                      <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6">05 // SECTOR HEATMAP</h3>
                      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-2">
                         {(data.usBriefing.sectorHeatmap || []).map((s, i) => (
                            <div key={i} className={`p-4 flex flex-col items-center justify-center text-center border transition-all ${
                               s.change.includes('+') 
                                 ? 'bg-success/10 border-success/30 hover:bg-success/20' 
                                 : s.change.includes('-') 
                                   ? 'bg-danger/10 border-danger/30 hover:bg-danger/20' 
                                   : 'bg-white/5 border-white/10'
                            }`}>
                               <span className="text-white font-bold text-xs mb-1">{s.sector}</span>
                               <span className={`text-sm font-black ${
                                  s.change.includes('+') ? 'text-success' : s.change.includes('-') ? 'text-danger' : 'text-gray-400'
                               }`}>{s.change}</span>
                            </div>
                         ))}
                      </div>
                   </section>

                   {/* 06. MIXED NEWS (Corporate/Politics) */}
                   <section>
                      <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6">06 // THE WIRE</h3>
                      <div className="space-y-3">
                         {(data.usBriefing.mixedNews || []).map((news, i) => (
                            <div key={i} className="flex items-start gap-4 p-4 border-b border-white/5 last:border-0 hover:bg-white/5 transition-colors">
                               <span className={`text-[9px] px-2 py-0.5 font-bold uppercase border mt-0.5 shrink-0 ${
                                  news.category === 'Politics' ? 'bg-purple-500/10 text-purple-400 border-purple-500/30' :
                                  news.category === 'Macro' ? 'bg-blue-500/10 text-blue-400 border-blue-500/30' :
                                  'bg-accent/10 text-accent border-accent/30'
                               }`}>{news.category}</span>
                               <span className="text-gray-300 font-sans text-sm">{news.headline}</span>
                            </div>
                         ))}
                      </div>
                   </section>
                   
                   {/* 07. WATCHLIST */}
                   <section className="bg-[#0A0A0A] border border-white/10 p-8">
                       <h3 className="text-[10px] font-black text-secondary uppercase tracking-[0.2em] mb-4">WATCHLIST</h3>
                       <div className="flex flex-wrap gap-4">
                          {(data.usBriefing.watchlist || []).map((w, i) => (
                             <div key={i} className="bg-white/5 px-4 py-2 border border-white/10">
                                <span className="font-bold text-white font-mono mr-2">{w.ticker}</span>
                                <span className="text-xs text-gray-500">{w.reason}</span>
                             </div>
                          ))}
                       </div>
                   </section>
                 </>
              ) : (

              /* ======================= STANDARD SECTOR INTELLIGENCE / INDONESIA ======================= */
              <>
                {/* 1. NARRATIVE CONCLUSION (Formerly Executive Summary) */}
                <section className="border-b border-white/5 pb-12">
                   <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6 font-sans">01 // {isMorningBriefing ? "MARKET OVERVIEW" : "INTELLIGENCE CONCLUSION"}</h3>
                   <div className="max-w-4xl mx-auto">
                     {data.headline && <h4 className="text-white font-bold text-3xl mb-6 font-serif italic text-center">"{data.headline}"</h4>}
                     <div className="text-lg md:text-xl text-gray-200 leading-8 font-serif text-justify prose prose-invert max-w-none">
                       {/* Render text with line breaks as paragraphs */}
                       {data.marketOverview.split('\n').map((para, idx) => (
                         <p key={idx} className="mb-4">{para}</p>
                       ))}
                     </div>
                   </div>
                </section>

                {/* 2. TOP NEWS */}
                <section className="border-b border-white/5 pb-12">
                   <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-8 font-sans">02 // TOP STORIES</h3>
                   <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                      {data.topNews.slice(0, 3).map((news, idx) => (
                         <div key={idx} className="bg-[#0A0A0A] border border-white/10 p-8 hover:border-accent/30 transition-all duration-300 group flex flex-col">
                            <h4 className="text-white font-bold text-lg mb-4 font-serif leading-tight group-hover:text-accent transition-colors">{news.headline}</h4>
                            <p className="text-gray-400 text-sm leading-6 font-sans text-justify flex-1">{news.description}</p>
                         </div>
                      ))}
                   </div>
                </section>

                {/* 3. SECTOR UPDATES (MORNING ONLY) */}
                {isMorningBriefing && data.sectorUpdates && (
                  <section className="border-b border-white/5 pb-12">
                     <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-8 font-sans">03 // SECTOR DEEP DIVE</h3>
                     <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
                        {Object.entries(data.sectorUpdates).map(([key, content], idx) => {
                           const sectorEnum = sectorMap[key];
                           const update = content as SectorUpdate;
                           return (
                             <div key={idx} className="border border-white/10 p-6 flex flex-col h-full bg-[#0A0A0A] hover:bg-[#0F0F0F] transition-colors group relative overflow-hidden min-h-[160px]">
                                <div className="flex justify-between items-center mb-4 border-b border-white/5 pb-2 relative z-10">
                                   <h4 className="text-white font-bold uppercase tracking-wider font-sans text-xs">{key}</h4>
                                   {sectorEnum ? (
                                     <button 
                                       onClick={() => { onSectorSelect(sectorEnum); onClose(); }} 
                                       className="text-[9px] text-accent border border-accent/20 px-2 py-1 uppercase hover:bg-accent hover:text-black transition-colors font-mono tracking-wider cursor-pointer"
                                     >
                                       Read Feed ↗
                                     </button>
                                   ) : <span className="text-[9px] text-gray-600 font-mono">NO FEED</span>}
                                </div>
                                <p className="text-gray-400 text-xs leading-6 font-sans flex-1 mb-2 relative z-10">{update.overview}</p>
                             </div>
                           );
                        })}
                     </div>
                  </section>
                )}

                {/* 4. DATA TABLES (Foreign Flow & Corp Action) */}
                {(data.foreignFlow || data.corporateActions) && (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-10 border-b border-white/5 pb-12">
                   {data.foreignFlow && (
                     <div className="glass-panel p-6 border border-white/10">
                        <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-4">FLOW ASING</h3>
                        <div className="flex justify-between items-center mb-6 border-b border-white/5 pb-4">
                           <span className="text-xs text-secondary font-mono">NET STATUS</span>
                           <span className={`text-lg font-bold font-mono ${data.foreignFlow.netStatus.includes('Buy') || data.foreignFlow.netStatus.includes('Masuk') ? 'text-success' : 'text-danger'}`}>{data.foreignFlow.netStatus}</span>
                        </div>
                        <div className="grid grid-cols-2 gap-6">
                           <div>
                              <p className="text-[9px] text-success mb-2 font-bold uppercase">TOP BUY</p>
                              {data.foreignFlow.topBuy.map((t, i) => (
                                 <div key={i} className="flex justify-between text-xs mb-1 font-mono"><span className="text-white">{t.ticker}</span><span className="text-gray-400">{t.value}</span></div>
                              ))}
                           </div>
                           <div>
                              <p className="text-[9px] text-danger mb-2 font-bold uppercase">TOP SELL</p>
                              {data.foreignFlow.topSell.map((t, i) => (
                                 <div key={i} className="flex justify-between text-xs mb-1 font-mono"><span className="text-white">{t.ticker}</span><span className="text-gray-400">{t.value}</span></div>
                              ))}
                           </div>
                        </div>
                     </div>
                   )}
                   {data.corporateActions && (
                      <div className="glass-panel p-6 border border-white/10">
                         <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-4">CORPORATE ACTION</h3>
                         <div className="overflow-x-auto">
                            <table className="w-full text-left">
                               <thead className="text-[9px] text-secondary uppercase font-mono border-b border-white/10"><tr><th className="pb-2">Emiten</th><th className="pb-2">Aksi</th><th className="pb-2 text-right">Tanggal</th></tr></thead>
                               <tbody className="text-xs font-mono">
                                  {data.corporateActions.slice(0, 5).map((act, i) => (
                                     <tr key={i} className="border-b border-white/5 last:border-0"><td className="py-2 text-accent font-bold">{act.ticker}</td><td className="py-2 text-white">{act.action}</td><td className="py-2 text-right text-gray-400">{act.date}</td></tr>
                                  ))}
                                  {data.corporateActions.length === 0 && <tr><td colSpan={3} className="py-4 text-center text-gray-500 italic">Tidak ada aksi korporasi besar.</td></tr>}
                               </tbody>
                            </table>
                         </div>
                      </div>
                   )}
                </div>
                )}

                {/* 5. STRATEGY (PLAYBOOK) */}
                <section>
                   <h3 className="text-[10px] font-black text-accent uppercase tracking-[0.2em] mb-6 font-sans">05 // STRATEGY & VERDICT</h3>
                   <div className="bg-[#0A0A0A] border border-accent/20 p-10 flex flex-col md:flex-row gap-10 items-start relative overflow-hidden">
                      <div className="md:w-1/3">
                         <p className="text-[10px] text-secondary uppercase mb-4 font-mono tracking-widest">WATCHLIST</p>
                         <div className="flex flex-wrap gap-3">
                            {data.watchlist.map((w, i) => (
                               <div key={i} className="bg-white/5 border border-white/10 px-4 py-3 hover:border-accent/50 transition-colors group">
                                  <span className="text-white font-bold font-mono block text-sm group-hover:text-accent">{w.ticker}</span>
                                  <span className="text-gray-500 text-[10px] block mt-1">{w.note}</span>
                               </div>
                            ))}
                         </div>
                      </div>
                      <div className="md:w-2/3 border-l border-white/10 pl-0 md:pl-10">
                         <p className="text-[10px] text-secondary uppercase mb-4 font-mono tracking-widest">VERDICT</p>
                         <h4 className="text-3xl font-black text-white mb-4 font-serif italic tracking-tight">"{data.recommendation.verdict}"</h4>
                         <p className="text-gray-300 text-sm leading-7 font-sans max-w-2xl mb-6">{data.recommendation.analysis}</p>
                      </div>
                   </div>
                </section>
              </>
              )}

              <div className="pt-8 flex justify-between items-center text-[10px] text-secondary font-mono border-t border-white/5">
                 <span className="uppercase tracking-widest">Generated by Velra Core AI • Sources below</span>
                 <div className="flex gap-4 flex-wrap justify-end">
                    {sources.map((s, i) => (
                       <a key={i} href={s.uri} target="_blank" className="hover:text-accent underline decoration-white/20 underline-offset-4 transition-colors">{s.title}</a>
                    ))}
                 </div>
              </div>

            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default BriefingModal;
