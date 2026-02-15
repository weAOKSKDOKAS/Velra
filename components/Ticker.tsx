import React from 'react';
import { NewsItem } from '../types';

interface TickerProps {
  newsItems: NewsItem[];
  loading: boolean;
}

const Ticker: React.FC<TickerProps> = ({ newsItems, loading }) => {
  return (
    <div className="h-10 flex items-center overflow-hidden w-full sticky top-0 z-50 bg-[#050505] border-b border-white/10 shadow-lg">
      <div className="bg-accent text-black h-full px-6 flex items-center font-black text-xs uppercase tracking-[0.2em] z-10 shadow-gold shrink-0">
        BREAKING NEWS
      </div>
      <div className="ticker-wrap flex-1 h-full flex items-center bg-black/80 backdrop-blur-sm">
        <div className="ticker flex items-center">
          {loading || !newsItems || newsItems.length === 0 ? (
            <span className="text-secondary font-mono text-xs px-4 tracking-widest animate-pulse">
              MENGHUBUNGKAN KE JARINGAN BERITA GLOBAL...
            </span>
          ) : (
            <>
              {newsItems.map((item, idx) => (
                <span key={idx} className="inline-flex items-center px-8 h-full border-r border-white/5">
                  <span className="text-accent font-bold text-xs font-mono mr-3">
                    {(item.time || 'NOW').toUpperCase()}
                  </span>
                  <span className="text-white font-sans text-sm font-medium tracking-wide">
                    {item.headline}
                  </span>
                </span>
              ))}
              {/* Duplicate for infinite loop effect */}
              {newsItems.map((item, idx) => (
                <span key={`dup-${idx}`} className="inline-flex items-center px-8 h-full border-r border-white/5">
                  <span className="text-accent font-bold text-xs font-mono mr-3">
                    {(item.time || 'NOW').toUpperCase()}
                  </span>
                  <span className="text-white font-sans text-sm font-medium tracking-wide">
                    {item.headline}
                  </span>
                </span>
              ))}
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default Ticker;