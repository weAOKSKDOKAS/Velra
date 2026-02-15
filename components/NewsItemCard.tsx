import React from 'react';
import { NewsItem } from '../types';

interface NewsItemCardProps {
  item: NewsItem;
  onReadMore?: () => void;
}

const NewsItemCard: React.FC<NewsItemCardProps> = ({ item, onReadMore }) => {
  const getImpactLabel = (impact: string) => {
    switch (impact) {
      case 'High': return 'TINGGI';
      case 'Medium': return 'SEDANG';
      case 'Low': return 'RENDAH';
      default: return impact;
    }
  };

  return (
    <div 
      className="glass-panel p-6 rounded-none hover:-translate-y-1 hover:border-accent/50 transition-all duration-300 group flex flex-col h-full shadow-lg hover:shadow-gold/20 border border-white/5 cursor-pointer relative overflow-hidden"
    >
      {/* Click handler wrapper for main content */}
      <div className="flex-1" onClick={onReadMore}>
          {/* Decorative corner flash */}
          <div className="absolute top-0 right-0 w-16 h-16 bg-gradient-to-bl from-accent/10 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none"></div>

          <div className="flex justify-between items-start mb-4 gap-3 relative z-10">
            <h3 className="text-white font-bold text-md leading-tight group-hover:text-accent transition-colors font-sans line-clamp-3">
              {item.headline}
            </h3>
            <span className={`shrink-0 text-[9px] px-2 py-0.5 rounded-sm font-bold uppercase tracking-wider border ${
              item.impact === 'High' 
                ? 'bg-danger/10 text-danger border-danger/30' 
                : item.impact === 'Medium' 
                  ? 'bg-warning/10 text-warning border-warning/30' 
                  : 'bg-success/10 text-success border-success/30'
            }`}>
              {getImpactLabel(item.impact)}
            </span>
          </div>
          
          <div className="flex items-center text-[10px] text-secondary mb-5 font-mono relative z-10">
            <span className="w-1 h-1 rounded-full bg-accent mr-2 opacity-80"></span>
            {item.time} WIB
          </div>

          <div className="space-y-3 mb-6 relative z-10">
            {item.summaryPoints.map((point, idx) => (
              <div key={idx} className="flex items-start">
                 <span className="text-accent mr-2 mt-1 text-[10px] opacity-60 group-hover:opacity-100 transition-opacity">▶</span>
                 <p className="text-xs text-gray-400 leading-relaxed group-hover:text-gray-300 transition-colors line-clamp-3">
                   {point}
                 </p>
              </div>
            ))}
          </div>
      </div>

      <div className="mt-auto pt-4 border-t border-white/5 relative z-10 flex justify-between items-center gap-2">
        {/* Source Link - Direct to External */}
        {item.sourceUrl && (
           <a 
             href={item.sourceUrl} 
             target="_blank" 
             rel="noopener noreferrer" 
             className="text-[9px] text-secondary hover:text-accent truncate max-w-[150px] underline decoration-white/10 underline-offset-2 hover:decoration-accent/50 transition-all"
             onClick={(e) => e.stopPropagation()} 
           >
             Sumber: {item.sourceName || 'Verifikasi Web'} ↗
           </a>
        )}

        {/* Read More Internal */}
        <div 
          onClick={onReadMore}
          className="w-6 h-6 rounded-full border border-white/10 flex items-center justify-center group-hover:border-accent group-hover:bg-accent group-hover:text-black transition-all duration-300 shrink-0 ml-auto"
        >
           <svg className="w-3 h-3 transform -rotate-45 group-hover:rotate-0 transition-transform duration-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
          </svg>
        </div>
      </div>
    </div>
  );
};

export default NewsItemCard;