import eventBus from '../eventBus.js';
import redisSimulator from '../cache/redisSimulator.js';
import inMemoryDB from '../db/inMemoryDB.js';

class CatalogService {
  /**
   * Implements the Proxy Pattern for product catalog reads.
   * Intercepts SQL database replica reads with a high-performance Redis cache.
   * @param {string} productId 
   * @returns {Promise<object|null>}
   */
  async getProduct(productId) {
    const cacheKey = `product:${productId}`;
    
    eventBus.log('DEV', 'SYSTEM', `[Catalog Service] Request GET /product/${productId} diterima`);

    // 1. Check Redis Cache
    const cachedData = redisSimulator.get(cacheKey);
    if (cachedData) {
      // CACHE HIT - Return immediately (extremely low latency)
      return {
        ...cachedData,
        _source: 'REDIS_CACHE',
        _latency: '0ms'
      };
    }

    // 2. CACHE MISS - Fallback to SQL Read Replica
    eventBus.log('DEV', 'SYSTEM', `[Catalog Service] Cache MISS untuk ${productId}. Membaca dari Read Replica...`);
    
    const dbProduct = await inMemoryDB.readFromReplica(productId);
    if (dbProduct) {
      // Set Cache in Redis with Jitter TTL (Default: 30s + 0-10s jitter)
      redisSimulator.set(cacheKey, dbProduct, 30);
      
      return {
        ...dbProduct,
        _source: 'DB_READ_REPLICA',
        _latency: '10-15ms' // Simulated DB query delay
      };
    }

    return null;
  }
}

const catalogService = new CatalogService();
export default catalogService;
