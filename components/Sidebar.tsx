
import React from 'react';
import { Region, Sector } from '../types';

interface SidebarProps {
  activeRegion: Region;
  setActiveRegion: (r: Region) => void;
  activeSector: Sector;
  setActiveSector: (s: Sector) => void;
  isOpen: boolean;
  onClose: () => void;
}

const Sidebar: React.FC<SidebarProps> = ({
  activeRegion,
  setActiveRegion,
  activeSector,
  setActiveSector,
  isOpen,
  onClose
}) => {
  
  const sidebarClasses = `
    fixed inset-y-0 left-0 z-[60] w-72 md:w-64 bg-[#050505] border-r border-white/10 flex flex-col transition-transform duration-300 ease-out shadow-2xl md:shadow-none
    md:sticky md:top-10 md:h-[calc(100vh-40px)] md:translate-x-0
    ${isOpen ? 'translate-x-0' : '-translate-x-full'}
  `;

  return (
    <>
      {isOpen && (
        <div 
          className="fixed inset-0 bg-black/80 z-[55] backdrop-blur-sm md:hidden animate-fade-in"
          onClick={onClose}
        ></div>
      )}

      <aside className={sidebarClasses}>
        <div className="p-6 border-b border-white/10 shrink-0 bg-black">
          <h1 className="text-xl font-black italic text-white flex items-center gap-2">
            VELRA<span className="text-[#d4af37]">TERMINAL</span>
          </h1>
          <p className="text-[9px] text-gray-500 font-mono tracking-widest uppercase mt-1">Market Intel v4.0</p>
        </div>

        <div className="overflow-y-auto custom-scrollbar flex-1 p-4 space-y-8">
          
          {/* REGION SECTION - PRIMARY BOX */}
          <div className="bg-[#0a0a0a] border border-white/5 rounded-lg overflow-hidden shadow-lg">
            <div className="bg-[#111] px-4 py-2 border-b border-white/10">
              <h3 className="text-[10px] font-black text-[#d4af37] uppercase tracking-[0.2em] flex items-center gap-2">
                <span className="w-1.5 h-1.5 bg-[#d4af37] rounded-full pulse"></span>
                Wilayah Pasar
              </h3>
            </div>
            <div className="p-2 grid grid-cols-1 gap-1">
              {Object.values(Region).map((region) => (
                <button
                  key={region}
                  onClick={() => setActiveRegion(region)}
                  className={`w-full text-left px-3 py-2.5 rounded text-xs font-bold transition-all duration-200 border ${
                    activeRegion === region
                      ? 'bg-[#d4af37] text-black border-[#d4af37] shadow-lg shadow-[#d4af37]/20'
                      : 'text-gray-400 border-transparent hover:text-white hover:bg-white/5'
                  }`}
                >
                  {region.toUpperCase()}
                </button>
              ))}
            </div>
          </div>

          {/* SECTOR SECTION - SECONDARY BOX */}
          <div className="bg-[#0a0a0a] border border-white/5 rounded-lg overflow-hidden shadow-lg">
            <div className="bg-[#111] px-4 py-2 border-b border-white/10">
              <h3 className="text-[10px] font-black text-gray-400 uppercase tracking-[0.2em] flex items-center gap-2">
                <span className="w-1.5 h-1.5 bg-gray-700 rounded-full"></span>
                Filter Sektor
              </h3>
            </div>
            <div className="p-2 grid grid-cols-1 gap-1">
              {Object.values(Sector).map((sector) => (
                <button
                  key={sector}
                  onClick={() => setActiveSector(sector)}
                  className={`w-full text-left px-3 py-2 rounded text-[11px] transition-all duration-200 border ${
                    activeSector === sector
                      ? 'bg-white/10 text-white border-white/20'
                      : 'text-gray-500 border-transparent hover:text-white hover:bg-white/5'
                  }`}
                >
                  {sector}
                </button>
              ))}
            </div>
          </div>

        </div>

        {/* BOTTOM STATUS */}
        <div className="p-4 mt-auto border-t border-white/5 bg-black/40">
           <div className="flex items-center gap-2 text-[9px] font-mono text-green-500">
              <div className="w-1.5 h-1.5 bg-green-500 rounded-full pulse"></div>
              <span className="tracking-widest uppercase">Live Matrix Link</span>
           </div>
           <div className="mt-1 text-[8px] text-gray-600 font-mono">
             SECURE_NODE_04 // PORT:5000
           </div>
        </div>
      </aside>
    </>
  );
};

export default Sidebar;
