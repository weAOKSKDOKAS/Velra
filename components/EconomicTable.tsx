import React from 'react';
import { EconomicIndicator } from '../types';
import { LineChart, Line, ResponsiveContainer, YAxis } from 'recharts';

interface EconomicTableProps {
  data: EconomicIndicator[];
}

const EconomicTable: React.FC<EconomicTableProps> = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="text-sm text-secondary italic p-4 text-center">Menunggu Data...</div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full">
        <thead>
          <tr className="text-[10px] uppercase text-accent font-bold tracking-widest text-left border-b border-white/5">
            <th className="py-3 pr-2 font-mono opacity-80">Metrik</th>
            <th className="py-3 px-2 w-24 text-center font-mono opacity-80">Tren</th>
            <th className="py-3 px-2 text-right font-mono opacity-80">Sblm</th>
            <th className="py-3 px-2 text-right text-white font-mono">Akt</th>
            <th className="py-3 px-2 text-right font-mono opacity-80">Est</th>
            <th className="py-3 pl-2 text-right text-accent font-mono">Lanjut</th>
          </tr>
        </thead>
        <tbody className="text-xs">
          {data.map((row, idx) => {
            // Robust check for history array to prevent crashes
            const sparkData = Array.isArray(row.history) 
              ? row.history.map((val, i) => ({ i, val: typeof val === 'number' ? val : 0 })) 
              : [];
              
            const lastVal = sparkData.length > 0 ? sparkData[sparkData.length - 1].val : 0;
            const prevVal = sparkData.length > 1 ? sparkData[sparkData.length - 2].val : 0;
            const trendColor = lastVal >= prevVal ? '#4ade80' : '#f87171';

            return (
              <tr key={idx} className="hover:bg-white/5 transition-colors group border-b border-white/5 last:border-0">
                <td className="py-4 pr-2 font-bold text-gray-300">{row.metric}</td>
                <td className="py-4 px-2">
                  <div className="h-6 w-20 mx-auto opacity-50 group-hover:opacity-100 transition-all duration-300 filter drop-shadow-lg">
                    {sparkData.length > 1 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={sparkData}>
                          <Line 
                            type="monotone" 
                            dataKey="val" 
                            stroke={trendColor} 
                            strokeWidth={1.5} 
                            dot={false}
                            isAnimationActive={false}
                          />
                          <YAxis domain={['dataMin', 'dataMax']} hide /> 
                        </LineChart>
                      </ResponsiveContainer>
                    ) : (
                      <span className="text-[10px] text-secondary block text-center">-</span>
                    )}
                  </div>
                </td>
                <td className="py-4 px-2 text-right text-secondary font-mono">{row.previous}</td>
                <td className="py-4 px-2 text-right font-bold text-white font-mono">
                   <span className="bg-surface-highlight px-2 py-1 rounded-sm border border-white/10">{row.actual}</span>
                </td>
                <td className="py-4 px-2 text-right text-secondary font-mono">{row.forecast}</td>
                <td className="py-4 pl-2 text-right text-accent font-mono font-bold">{row.nextRelease}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export default EconomicTable;