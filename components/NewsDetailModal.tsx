
import React from 'react';
import { NewsArticle, NewsItem } from '../types';

interface NewsDetailModalProps {
  isOpen: boolean;
  onClose: () => void;
  article: NewsArticle | null;
  newsItem: NewsItem | null;
  loading: boolean;
}

const NewsDetailModal: React.FC<NewsDetailModalProps> = ({ isOpen, onClose, article, newsItem, loading }) => {
  if (!isOpen) return null;

  // Use article sources if verified, fallback to the item's sourceUrl which is now strictly validated in geminiService
  const sources = article?.sources && article.sources.length > 0 
    ? article.sources 
    : (newsItem?.sourceUrl ? [{ title: newsItem.sourceName || 'Sumber Berita Utama', uri: newsItem.sourceUrl }] : []);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-0 md:p-6">
      <div className="absolute inset-0 bg-black/90 backdrop-blur-md animate-fade-in" onClick={onClose}></div>

      <div className="relative w-full md:max-w-5xl h-full md:h-[90vh] bg-[#0A0A0A] border border-white/10 shadow-2xl flex flex-col overflow-hidden animate-fade-in md:rounded-xl ring-1 ring-white/5">
        
        <div className="h-10 bg-surface-highlight border-b border-white/10 flex justify-between items-center px-4 shrink-0 select-none">
           <div className="flex items-center gap-2">
              <div className="flex gap-1.5">
                 <div className="w-2.5 h-2.5 rounded-full bg-red-500/20 border border-red-500/50"></div>
                 <div className="w-2.5 h-2.5 rounded-full bg-yellow-500/20 border border-yellow-500/50"></div>
                 <div className="w-2.5 h-2.5 rounded-full bg-green-500/20 border border-green-500/50"></div>
              </div>
              <span className="ml-3 text-[10px] font-mono text-secondary uppercase tracking-widest opacity-60">
                 VELRA TERMINAL // VERIFIED WIRE
              </span>
           </div>
           <button onClick={onClose} className="text-secondary hover:text-white transition-colors p-1">
             <span className="text-xs font-mono font-bold">[ESC] CLOSE</span>
           </button>
        </div>

        <div className="flex-1 overflow-y-auto custom-scrollbar bg-[#050505]">
            {loading ? (
                <div className="p-8 md:p-16 space-y-8 animate-pulse max-w-4xl mx-auto">
                    <div className="space-y-4">
                       <div className="h-2 bg-accent/20 w-32 rounded-full"></div>
                       <div className="h-10 bg-white/10 w-3/4 rounded-sm"></div>
                    </div>
                    <div className="h-64 bg-white/5 rounded-sm border border-white/5 w-full"></div>
                    <div className="space-y-4 pt-8">
                       {[1,2,3,4,5].map(i => <div key={i} className="h-3 bg-white/10 w-full rounded-sm"></div>)}
                    </div>
                </div>
            ) : article ? (
                <article className="max-w-4xl mx-auto p-6 md:p-12 text-gray-200">
                    <header className="mb-10 border-b border-white/10 pb-10">
                        <div className="flex items-center gap-3 mb-6">
                           <span className="bg-accent text-black text-[9px] font-black px-1.5 py-0.5 uppercase tracking-widest">VERIFIED</span>
                           <span className="text-[10px] text-accent font-mono uppercase tracking-widest">
                             {article.timestamp}
                           </span>
                        </div>
                        <h1 className="text-3xl md:text-5xl font-black text-white mb-6 leading-tight tracking-tight font-serif italic">
                            {article.headline}
                        </h1>
                        <div className="flex items-center justify-between font-mono text-xs text-secondary border-t border-white/5 pt-4">
                           <div className="flex items-center gap-2">
                              <span className="uppercase tracking-wider">Editor:</span>
                              <span className="text-white font-bold">{article.author || 'Velra Global Desk'}</span>
                           </div>
                           <div className="flex items-center gap-2">
                              <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse"></span>
                              <span>LIVE VERIFICATION</span>
                           </div>
                        </div>
                    </header>

                    <div className="grid grid-cols-1 lg:grid-cols-12 gap-10">
                        <div className="lg:col-span-5 space-y-8">
                            <div className="bg-surface-highlight/30 border border-accent/20 p-6 relative overflow-hidden">
                                <div className="absolute top-0 left-0 w-1 h-full bg-accent"></div>
                                <h3 className="text-[10px] font-black uppercase tracking-[0.2em] text-accent mb-4">5W1H SUMMARY</h3>
                                <ul className="space-y-4">
                                    {article.takeaways?.map((point, i) => (
                                        <li key={i} className="flex gap-3 text-sm font-medium leading-relaxed text-gray-300">
                                            <span className="text-accent text-[10px] mt-1.5 font-bold">◆</span>
                                            {point}
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        </div>
                        <div className="lg:col-span-7 prose prose-invert prose-lg max-w-none font-serif">
                            {(article.content || '').split(/\n\s*\n/).map((paragraph, index) => (
                                // UNIFIED FONT STYLING:
                                // All paragraphs use the same class. No differentiation for the first one.
                                // Text color is specifically set to text-gray-200 (bright white-gray).
                                <p key={index} className="mb-6 leading-relaxed font-normal text-[1.125rem] text-justify text-gray-200">
                                    {paragraph}
                                </p>
                            ))}
                        </div>
                    </div>

                    <footer className="mt-16 pt-8 border-t border-white/10">
                       <h4 className="text-[10px] font-black text-secondary uppercase tracking-[0.2em] mb-4">SUMBER VERIFIKASI (KLIK UNTUK MEMBACA ASLINYA)</h4>
                       <div className="flex flex-wrap gap-3">
                          {sources.map((source, idx) => (
                                <a 
                                  key={idx} 
                                  href={source.uri} 
                                  target="_blank" 
                                  rel="noopener noreferrer" 
                                  className="flex items-center gap-2 bg-white/5 border border-white/10 px-4 py-3 rounded-sm hover:bg-accent hover:text-black hover:border-accent transition-all duration-300 group cursor-pointer"
                                >
                                    <span className="text-xs font-mono font-bold">{source.title}</span>
                                    <span className="text-[10px] opacity-50 group-hover:opacity-100">↗</span>
                                </a>
                           ))}
                           {sources.length === 0 && (
                             <div className="text-xs text-red-400 font-mono">
                               ⚠️ Sumber spesifik tidak ditemukan. Verifikasi manual disarankan.
                             </div>
                           )}
                       </div>
                    </footer>
                </article>
            ) : null}
        </div>
      </div>
    </div>
  );
};

export default NewsDetailModal;
