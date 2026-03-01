/**
 * Dry Run Example - Mock API + SQLite + Prisma
 * 
 * This demonstrates a complete ingestion pipeline with:
 * - Mock HTTP API (serves sample data)
 * - SQLite database (via Prisma)
 * - Dry run mode (just shows what would be inserted)
 */

import { PrismaClient } from '@prisma/client';
import * as http from 'http';

// Mock API data
const MOCK_TRADES = [
  { tradeId: 'T001', price: 45000.50, quantity: 0.5, executedAt: '2024-01-15T10:30:00Z', symbol: 'BTC', side: 'buy' },
  { tradeId: 'T002', price: 45100.25, quantity: 1.2, executedAt: '2024-01-15T10:31:00Z', symbol: 'BTC', side: 'sell' },
  { tradeId: 'T003', price: 45200.00, quantity: 0.8, executedAt: '2024-01-15T10:32:00Z', symbol: 'ETH', side: 'buy' },
  { tradeId: 'T004', price: 2800.50, quantity: 5.0, executedAt: '2024-01-15T10:33:00Z', symbol: 'ETH', side: 'sell' },
  { tradeId: 'T005', price: 45150.75, quantity: 0.3, executedAt: '2024-01-15T10:34:00Z', symbol: 'BTC', side: 'buy' },
];

const MOCK_MARKETS = [
  { symbol: 'BTC', name: 'Bitcoin', base: 'BTC', quote: 'USD', lastPrice: 45000, volume: 1000000, status: 'active' },
  { symbol: 'ETH', name: 'Ethereum', base: 'ETH', quote: 'USD', lastPrice: 2800, volume: 500000, status: 'active' },
  { symbol: 'SOL', name: 'Solana', base: 'SOL', quote: 'USD', lastPrice: 120, volume: 200000, status: 'active' },
];

/**
 * Start mock API server
 */
function startMockServer(): Promise<number> {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      res.setHeader('Content-Type', 'application/json');
      
      if (req.url === '/v1/trades') {
        res.end(JSON.stringify({ data: MOCK_TRADES }));
      } else if (req.url === '/v1/markets') {
        res.end(JSON.stringify({ markets: MOCK_MARKETS }));
      } else {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: 'Not found' }));
      }
    });
    
    server.listen(0, () => {
      const port = (server.address() as any).port;
      console.log(`📡 Mock API server running on port ${port}`);
      resolve(port);
    });
  });
}

/**
 * Database writer using Prisma
 */
async function writeToDatabase(
  records: any[],
  model: string,
  options: any
): Promise<void> {
  const prisma = new PrismaClient();
  
  try {
    if (model === 'Trade') {
      for (const record of records) {
        await prisma.trade.upsert({
          where: { id: record.id },
          update: record,
          create: record,
        });
      }
      console.log(`   ✅ Inserted/updated ${records.length} Trade records`);
    } else if (model === 'Market') {
      // Market uses symbol as id
      for (const record of records) {
        await prisma.market.upsert({
          where: { id: record.symbol },
          update: record,
          create: { ...record, id: record.symbol },
        });
      }
      console.log(`   ✅ Inserted/updated ${records.length} Market records`);
    }
  } finally {
    await prisma.$disconnect();
  }
}

/**
 * Transform function (simplified from transforms registry)
 */
function transform(value: any, transformName: string): any {
  if (value == null) return value;
  
  switch (transformName) {
    case 'toFloat':
      return Number(value);
    case 'toUpperCase':
      return String(value).toUpperCase();
    case 'toDate':
      return new Date(value);
    default:
      return value;
  }
}

/**
 * Apply mapping to records
 */
function applyMapping(records: any[], mapping: any): any[] {
  return records.map(record => {
    const result: any = {};
    for (const [field, config] of Object.entries(mapping)) {
      let value = record;
      const path = (config as any).from.replace('$.', '').split('.');
      for (const p of path) {
        value = value?.[p];
      }
      if (value === undefined && (config as any).default !== undefined) {
        value = (config as any).default;
      }
      if ((config as any).transform) {
        value = transform(value, (config as any).transform);
      }
      result[field] = value;
    }
    return result;
  });
}

/**
 * Run dry-run example
 */
async function main() {
  console.log('🧪 Prisma-Bound YAML Ingestion - Dry Run Example\n');
  console.log('='.repeat(50));
  
  // Start mock API server
  const port = await startMockServer();
  const baseUrl = `http://localhost:${port}`;
  
  // Trade pipeline config (as would be defined in YAML)
  const tradePipeline = {
    model: 'Trade',
    source: {
      type: 'http' as const,
      url: `${baseUrl}/v1/trades`,
      method: 'GET' as const,
      jsonPath: '$.data',
    },
    mapping: {
      id: { from: '$.tradeId' },
      price: { from: '$.price', transform: 'toFloat' },
      amount: { from: '$.quantity', transform: 'toFloat' },
      timestamp: { from: '$.executedAt', transform: 'toDate' },
      symbol: { from: '$.symbol' },
      side: { from: '$.side' },
      status: { from: '$.side', transform: 'toUpperCase' },
    },
    options: {
      batchSize: 100,
      onConflict: 'update' as const,
    },
  };
  
  const marketPipeline = {
    model: 'Market',
    source: {
      type: 'http' as const,
      url: `${baseUrl}/v1/markets`,
      method: 'GET' as const,
      jsonPath: '$.markets',
    },
    mapping: {
      symbol: { from: '$.symbol' },
      name: { from: '$.name' },
      baseAsset: { from: '$.base' },
      quoteAsset: { from: '$.quote' },
      price: { from: '$.lastPrice', transform: 'toFloat' },
      volume24h: { from: '$.volume', transform: 'toFloat' },
      status: { from: '$.status' },
    },
    options: {
      batchSize: 100,
      onConflict: 'update' as const,
    },
  };
  
  // Demo: Dry run mode (show what would be inserted without writing)
  console.log('\n📦 DRY RUN MODE - No database writes\n');
  
  // Show trade pipeline data
  console.log('Trade Pipeline - Source Data:');
  console.log(JSON.stringify(MOCK_TRADES, null, 2));
  
  console.log('\nTrade Pipeline - Transformed Data (what would be inserted):');
  const transformedTrades = applyMapping(MOCK_TRADES, tradePipeline.mapping);
  console.log(JSON.stringify(transformedTrades, null, 2));
  
  console.log('\n' + '-'.repeat(50));
  
  console.log('\nMarket Pipeline - Source Data:');
  console.log(JSON.stringify(MOCK_MARKETS, null, 2));
  
  console.log('\nMarket Pipeline - Transformed Data (what would be inserted):');
  const transformedMarkets = applyMapping(MOCK_MARKETS, marketPipeline.mapping);
  console.log(JSON.stringify(transformedMarkets, null, 2));
  
  // Now actually run with database
  console.log('\n' + '='.repeat(50));
  console.log('🚀 Running actual ingestion with SQLite database...\n');
  
  // Run Trade ingestion
  console.log('📥 Ingesting Trades...');
  await writeToDatabase(transformedTrades, 'Trade', tradePipeline.options);
  
  // Run Market ingestion
  console.log('\n📥 Ingesting Markets...');
  await writeToDatabase(transformedMarkets, 'Market', marketPipeline.options);
  
  // Show database contents
  console.log('\n📊 Database Contents:');
  const prisma = new PrismaClient();
  const trades = await prisma.trade.findMany();
  const markets = await prisma.market.findMany();
  console.log(`   Trades: ${trades.length} records`);
  console.log(`   Markets: ${markets.length} records`);
  
  console.log('\n   Trade Records:');
  console.log(JSON.stringify(trades, null, 2));
  
  console.log('\n   Market Records:');
  console.log(JSON.stringify(markets, null, 2));
  
  await prisma.$disconnect();
  
  console.log('\n✅ Dry run example complete!');
  process.exit(0);
}

main().catch(console.error);
