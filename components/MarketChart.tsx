import React from 'react';
import { ResponsiveContainer, LineChart, Line, YAxis } from 'recharts';
import { MarketIndex } from '../types';

interface MarketDashboardProps {
  indices: MarketIndex[];
  loading: boolean;
  region: string;
}

// Generate a synthetic curve that visually matches the trend (UP/DOWN)
// Enhanced for more realistic random walk behavior
const generateSyntheticCurve = (trend: 'UP' | 'DOWN' | 'FLAT', volatility = 1.0) => {
  const points = [];
  const steps = 40; // More points for smoother line
  let currentVal = 100;
  
  // Determine start and end targets based on trend
  // If UP: Start lower, End higher.
  // If DOWN: Start higher, End lower.
  // If FLAT: Start and End similar.
  const startVal = 100;
  const endVal = trend === 'UP' ? 110 : trend === 'DOWN' ? 90 : 100;
  
  const slope = (endVal - startVal) / steps;

  for (let i = 0; i < steps; i++) {
    // Random walk component
    const noise = (Math.random() - 0.5) * volatility * 3;
    
    // Trend component
    currentVal = startVal + (slope * i) + noise;
    
    // Ensure we don't drift too far from trend line
    // (simple mean reversion to trend line)
    const trendLineVal = startVal + (slope * i);
    currentVal = (currentVal + trendLineVal) / 2;

    points.push({ val: currentVal });
  }
  return points;
};

const MarketCard: React.FC<{ index: MarketIndex }> = ({ index }) => {
  const isUp = index.trend === 'UP';
  const color = isUp ? '#4ade80' : '#f87171'; // Green or Red
  const chartData = generateSyntheticCurve(index.trend, 1.5);

  return (
    <div className="glass-panel p-5 rounded-none border border-white/5 relative overflow-hidden group hover:border-white/10 transition-all duration-300 flex flex-col justify-between h-[140px]">
      {/* Header info */}
      <div className="flex justify-between items-start mb-2 relative z-10">
        <div>
           <h3 className="text-sm font-bold text-white font-mono tracking-wider mb-1">{index.symbol}</h3>
           <p className="text-[10px] text-secondary uppercase tracking-widest truncate max-w-[120px]">{index.name}</p>
        </div>
        <div className={`text-right ${isUp ? 'text-success' : 'text-danger'}`}>
           <p className="text-xs font-bold font-mono">{index.percentChange}</p>
           <p className="text-[10px] font-mono opacity-80">{index.change}</p>
        </div>
      </div>
      
      <div className="flex items-end justify-between relative z-10 mt-auto mb-1">
         <span className="text-2xl font-black text-white font-mono tracking-tight">
           {index.price}
         </span>
      </div>

      {/* Sparkline Chart Area */}
      <div className="absolute bottom-2 left-0 right-0 h-12 opacity-50 group-hover:opacity-80 transition-opacity z-0 pointer-events-none px-2">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
             <Line 
               type="monotone" 
               dataKey="val" 
               stroke={color} 
               strokeWidth={2} 
               dot={false}
               isAnimationActive={true}
             />
             <YAxis domain={['dataMin', 'dataMax']} hide />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

const MarketDashboard: React.FC<MarketDashboardProps> = ({ indices, loading, region }) => {
  const currentHour = new Date().getHours();
  const nextHour = (currentHour + 1) % 24;

  if (loading) {
     return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
           {[1,2,3,4,5].map(i => (
              <div key={i} className="h-[140px] bg-surface-highlight/20 animate-pulse border border-white/5 rounded-none"></div>
           ))}
        </div>
     );
  }

  if (!indices || indices.length === 0) return null;

  return (
    <div className="space-y-4">
       <div className="flex justify-between items-end border-b border-white/5 pb-2">
          <h2 className="text-sm font-bold text-accent uppercase tracking-widest flex items-center gap-2">
             <span className="w-2 h-2 rounded-full bg-accent animate-pulse"></span>
             Pasar {region}
          </h2>
          <div className="text-[10px] font-mono text-secondary">
             Updated: {String(currentHour).padStart(2,'0')}:00 | Next: {String(nextHour).padStart(2,'0')}:00
          </div>
       </div>
       <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
          {indices.map((idx, i) => (
             <MarketCard key={i} index={idx} />
          ))}
       </div>
    </div>
  );
};

export default MarketDashboard;
