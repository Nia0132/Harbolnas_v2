import eventBus from '../eventBus.js';
import catalogService from '../services/catalogService.js';
import transactionService from '../services/transactionService.js';

class ApiGateway {
  constructor() {
    this.readLimits = new Map(); // IP -> { count, lastResetTime }
    this.writeLimits = new Map(); // Username -> { lastRequestTime }
    this.circuitState = 'CLOSED'; // 'CLOSED', 'OPEN'
    this.consecutiveFailures = 0;
    this.failureThreshold = 3;
    this.fallbackProduct = {
      id: 'iphone',
      name: 'iPhone 15 Pro Max (Offline Cache)',
      price: 10000,
      originalPrice: 20000000,
      stock: 'Unknown',
      _source: 'GATEWAY_FALLBACK',
      _latency: '0ms (Circuit Breaker OPEN)'
    };
  }

  /**
   * Routes and sanitizes incoming HTTP/API requests, enforcing rate limits and circuit breaking.
   * @param {'GET' | 'POST'} method 
   * @param {string} path - The request path (e.g. '/product/iphone' or '/checkout')
   * @param {object} payload - Request payload (contains ip, username, productId, paymentMethod)
   * @returns {Promise<object>} Response payload
   */
  async route(method, path, payload) {
    const ip = payload.ip || '127.0.0.1';
    const username = payload.username || 'guest';
    const productId = payload.productId || 'iphone';

    eventBus.log('DEV', 'GATEWAY', `[API Gateway] Routing ${method} ${path} untuk User: ${username} (IP: ${ip})`);

    // 1. Enforce Circuit Breaker
    if (this.circuitState === 'OPEN') {
      eventBus.log('DEV', 'GATEWAY', `[Circuit Breaker OPEN ⚠️] Memblokir request ke backend. Mengembalikan data fallback lokal.`, {
        circuitState: 'OPEN'
      });
      // Return static local product copy immediately (degraded mode)
      if (method === 'GET' && path.startsWith('/product')) {
        return { success: true, data: this.fallbackProduct };
      }
      throw new Error('Sistem sedang dalam pemeliharaan darurat. Silakan coba sesaat lagi.');
    }

    // 2. Enforce Rate Limiting
    if (method === 'GET' && path.startsWith('/product')) {
      const allowed = this.checkReadRateLimit(ip);
      if (!allowed) {
        const errorMsg = 'Batas kuota akses tercapai! Maksimal 10 request/detik per IP.';
        eventBus.log('DEV', 'GATEWAY', `[Rate Limiter BLOCKED ❌] GET ditolak untuk IP: ${ip} (Limit: 10 req/s)`);
        throw new Error(errorMsg);
      }

      // Route to Catalog Service (Read)
      try {
        const product = await catalogService.getProduct(productId);
        this.resetFailureCount();
        return { success: true, data: product };
      } catch (err) {
        this.handleFailure();
        throw err;
      }

    } else if (method === 'POST' && path === '/checkout') {
      const allowed = this.checkWriteRateLimit(username);
      if (!allowed) {
        const errorMsg = 'Transaksi terlalu cepat! Maksimal 1 checkout/detik per Pengguna.';
        eventBus.log('DEV', 'GATEWAY', `[Rate Limiter BLOCKED ❌] POST checkout ditolak untuk User: ${username} (Limit: 1 req/s)`);
        throw new Error(errorMsg);
      }

      // Route to Transaction Service (Write)
      try {
        const tx = await transactionService.checkout(username, productId, payload.paymentMethod, ip);
        this.resetFailureCount();
        return { success: true, data: tx };
      } catch (err) {
        // If stock is sold out, it is business logic (not system failure), don't trip breaker
        if (err.message !== 'Maaf, produk sudah terjual habis!' && err.message !== 'STOK_HABIS' && !err.message.includes('DITOLAK')) {
          this.handleFailure();
        }
        throw err;
      }
    }

    throw new Error('Endpoint tidak ditemukan.');
  }

  /**
   * Rate limiting logic for Product Reads (Max 10 requests per 1 second per IP)
   * @param {string} ip 
   * @returns {boolean} True if allowed, false if blocked
   */
  checkReadRateLimit(ip) {
    const now = Date.now();
    
    if (!this.readLimits.has(ip)) {
      this.readLimits.set(ip, { count: 1, lastResetTime: now });
      return true;
    }

    const limitInfo = this.readLimits.get(ip);
    
    if (now - limitInfo.lastResetTime > 1000) {
      // 1 second window passed, reset bucket
      limitInfo.count = 1;
      limitInfo.lastResetTime = now;
      return true;
    }

    if (limitInfo.count >= 10) {
      return false; // throttled
    }

    limitInfo.count += 1;
    return true;
  }

  /**
   * Rate limiting logic for Checkouts (Max 1 checkout request per 1 second per User ID)
   * @param {string} username 
   * @returns {boolean} True if allowed, false if blocked
   */
  checkWriteRateLimit(username) {
    const now = Date.now();

    if (!this.writeLimits.has(username)) {
      this.writeLimits.set(username, { lastRequestTime: now });
      return true;
    }

    const limitInfo = this.writeLimits.get(username);
    if (now - limitInfo.lastRequestTime < 1000) {
      return false; // throttled (must wait 1s between writes)
    }

    limitInfo.lastRequestTime = now;
    return true;
  }

  /**
   * Handles consecutive backend failure tracking.
   */
  handleFailure() {
    this.consecutiveFailures += 1;
    eventBus.log('DEV', 'GATEWAY', `[Gateway Warning ⚠️] Deteksi kegagalan layanan backend (${this.consecutiveFailures}/${this.failureThreshold})`);
    
    if (this.consecutiveFailures >= this.failureThreshold) {
      this.tripCircuit();
    }
  }

  /**
   * Trips the circuit breaker to OPEN state.
   */
  tripCircuit() {
    this.circuitState = 'OPEN';
    eventBus.log('DEV', 'GATEWAY', '🚨 [CIRCUIT BREAKER OPENED] Downstream service tidak stabil! Membuka circuit untuk mengamankan resource backend.');
  }

  /**
   * Resets the circuit breaker back to CLOSED state.
   */
  resetCircuit() {
    this.circuitState = 'CLOSED';
    this.consecutiveFailures = 0;
    eventBus.log('DEV', 'GATEWAY', '✓ [CIRCUIT BREAKER CLOSED] Keadaan sistem kembali stabil. Menghubungkan ulang ke backend.');
  }

  resetFailureCount() {
    this.consecutiveFailures = 0;
  }

  /**
   * Returns gateway metrics for UI rendering.
   */
  getGatewayState() {
    return {
      circuitState: this.circuitState,
      consecutiveFailures: this.consecutiveFailures,
      failureThreshold: this.failureThreshold,
      activeIpsMonitored: this.readLimits.size
    };
  }
}

const apiGateway = new ApiGateway();
export default apiGateway;
