import eventBus from '../eventBus.js';

class RedisSimulator {
  constructor() {
    this.cache = new Map();
  }

  /**
   * Retrieves an item from the cache.
   * Performs lazy eviction if the item has expired.
   * @param {string} key 
   * @returns {any} Cached value, or null if miss
   */
  get(key) {
    if (!this.cache.has(key)) {
      eventBus.log('DEV', 'REDIS', `[Cache MISS] Kunci: ${key} - Tidak ada di cache`, { isHit: false });
      return null;
    }

    const entry = this.cache.get(key);
    const now = Date.now();

    if (now > entry.expiresAt) {
      this.cache.delete(key);
      eventBus.log('DEV', 'REDIS', `[Cache EXPIRED] Kunci: ${key} - Dihapus secara otomatis (Lazy Eviction)`, { isHit: false });
      return null;
    }

    eventBus.log('DEV', 'REDIS', `[Cache HIT] Kunci: ${key} - Mengambil data instant (Latency: 0-1ms)`, {
      isHit: true,
      ttlRemaining: Math.max(0, Math.round((entry.expiresAt - now) / 1000))
    });
    return entry.value;
  }

  /**
   * Sets a value in the cache with dynamic Jitter TTL to prevent Cache Stampede.
   * @param {string} key 
   * @param {any} value 
   * @param {number} [baseTTLSeconds=30] - Base Time To Live in seconds
   */
  set(key, value, baseTTLSeconds = 30) {
    const jitter = Math.floor(Math.random() * 11); // random 0-10 seconds jitter
    const finalTTLSeconds = baseTTLSeconds + jitter;
    const expiresAt = Date.now() + (finalTTLSeconds * 1000);

    this.cache.set(key, {
      value,
      expiresAt,
      baseTTL: baseTTLSeconds,
      jitter: jitter
    });

    eventBus.log('DEV', 'REDIS', `[Cache SET] Kunci: ${key} disimpan. TTL: ${finalTTLSeconds}s (Base: ${baseTTLSeconds}s + Jitter: ${jitter}s)`, {
      ttl: finalTTLSeconds,
      baseTTL: baseTTLSeconds,
      jitter: jitter
    });
  }

  /**
   * Deletes a key from the cache (used in Cache Invalidation).
   * @param {string} key 
   */
  delete(key) {
    if (this.cache.has(key)) {
      this.cache.delete(key);
      eventBus.log('DEV', 'REDIS', `[Cache INVALIDATE] Kunci: ${key} dihapus dari cache (Push Invalidation)`, {
        invalidated: true
      });
      return true;
    }
    return false;
  }

  /**
   * Clears the entire cache.
   */
  clear() {
    this.cache.clear();
    eventBus.log('DEV', 'REDIS', 'Cache Redis berhasil dibersihkan.');
  }

  /**
   * Returns metadata about active cache entries for UI rendering.
   * @returns {Array<object>}
   */
  getKeysState() {
    const now = Date.now();
    const states = [];

    for (const [key, entry] of this.cache.entries()) {
      if (now <= entry.expiresAt) {
        states.push({
          key,
          value: entry.value,
          ttlRemainingMs: entry.expiresAt - now,
          ttlTotalMs: (entry.baseTTL + entry.jitter) * 1000,
          percentRemaining: Math.round(((entry.expiresAt - now) / ((entry.baseTTL + entry.jitter) * 1000)) * 100)
        });
      } else {
        // cleanup expired on-the-fly
        this.cache.delete(key);
      }
    }
    return states;
  }
}

const redisSimulator = new RedisSimulator();
export default redisSimulator;
