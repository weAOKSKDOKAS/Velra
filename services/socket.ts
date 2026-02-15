import { MarketUpdate } from '../types';

type Listener = (updates: MarketUpdate[]) => void;

interface MarketState {
  price: number;
  basePrice: number;
  volatility: number;
}

// Initial configuration for simulated assets - UPDATED TO 2025 BULL MARKET CONTEXT (IHSG ~8700)
const INITIAL_MARKET_STATE: Record<string, MarketState> = {
  // Indonesia (IHSG & Bluechips)
  'IHSG': { price: 8724.55, basePrice: 8724.55, volatility: 12.5 }, // Sesuai permintaan
  'BBCA': { price: 12450, basePrice: 12450, volatility: 75 }, // Valuasi menyesuaikan IHSG 8700
  'BBRI': { price: 6850, basePrice: 6850, volatility: 45 },
  'BMRI': { price: 8200, basePrice: 8200, volatility: 50 },
  'TLKM': { price: 3450, basePrice: 3450, volatility: 25 },
  'GOTO': { price: 50, basePrice: 50, volatility: 1 }, // Tetap di gocap (realistis/pesimistis) atau bangkit sedikit
  'ASII': { price: 6100, basePrice: 6100, volatility: 35 },
  'BREN': { price: 11250, basePrice: 11250, volatility: 150 }, // Volatile stock
  
  // US Indices (Projection for late 2025)
  'S&P 500': { price: 6850.40, basePrice: 6850.40, volatility: 8.5 },
  'NASDAQ': { price: 22400.10, basePrice: 22400.10, volatility: 25.5 },
  'DOW JONES': { price: 48100.00, basePrice: 48100.00, volatility: 20.0 },
  
  // Regional Indices
  'N225': { price: 42500.50, basePrice: 42500.50, volatility: 120.0 }, // Nikkei Bull run
  'STOXX50': { price: 5200.25, basePrice: 5200.25, volatility: 8.0 },
  'BVSP': { price: 135000.00, basePrice: 135000.00, volatility: 350.0 },
  
  // Commodities & Forex
  'USD/IDR': { price: 15450, basePrice: 15450, volatility: 15 }, // Rupiah menguat dikit di skenario bullish IHSG
  'GOLD': { price: 2850.50, basePrice: 2850.50, volatility: 2.5 },
  'OIL (WTI)': { price: 72.40, basePrice: 72.40, volatility: 0.6 },
  'BTC/USD': { price: 102500.00, basePrice: 102500.00, volatility: 500.0 }, // Crypto context
};

class MockWebSocketService {
  private listeners: Set<Listener> = new Set();
  private intervalId: any = null;
  private marketData: Map<string, MarketState> = new Map();
  private isConnected: boolean = false;

  constructor() {
    // Initialize market data
    Object.entries(INITIAL_MARKET_STATE).forEach(([symbol, config]) => {
      this.marketData.set(symbol, { ...config });
    });
  }

  // Simulate opening a connection
  connect() {
    if (this.isConnected) return;
    this.isConnected = true;
    console.log('[Socket] Connected to Market Data Feed');
    
    // Simulate incoming messages every 1 second for smoother ticker
    this.intervalId = setInterval(() => {
      this.tick();
    }, 1000);
  }

  // Simulate closing a connection
  disconnect() {
    if (!this.isConnected) return;
    this.isConnected = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    console.log('[Socket] Disconnected');
  }

  subscribe(callback: Listener) {
    this.listeners.add(callback);
    // Send immediate snapshot upon subscription
    const snapshot = this.generateUpdates();
    callback(snapshot);
    
    return () => {
      this.listeners.delete(callback);
    };
  }

  private tick() {
    const updates = this.generateUpdates();
    this.notify(updates);
  }

  private generateUpdates(): MarketUpdate[] {
    const updates: MarketUpdate[] = [];
    const timestamp = new Date().toISOString();

    this.marketData.forEach((state, symbol) => {
      // Random walk algorithm
      // Volatility logic: slight bias to trend upwards in this bullish scenario
      const drift = 0.002; 
      const change = (Math.random() - 0.5 + drift) * state.volatility;
      let newPrice = state.price + change;
      
      // Ensure price doesn't go negative
      if (newPrice < 0.01) newPrice = 0.01;

      // Update internal state
      state.price = newPrice;

      // Calculate stats
      const diff = newPrice - state.basePrice;
      const changePercent = (diff / state.basePrice) * 100;

      updates.push({
        symbol,
        price: Number(newPrice.toFixed(2)),
        change: Number(diff.toFixed(2)),
        changePercent: Number(changePercent.toFixed(2)),
        timestamp,
      });
    });

    return updates;
  }

  private notify(updates: MarketUpdate[]) {
    this.listeners.forEach((listener) => listener(updates));
  }

  // Helper to get a single current value (useful for initial chart state)
  getCurrentPrice(symbol: string): number {
    return this.marketData.get(symbol)?.price || 0;
  }
}

// Export singleton
export const socketService = new MockWebSocketService();