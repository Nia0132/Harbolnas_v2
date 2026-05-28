import eventBus from '../eventBus.js';

class DistributedLock {
  constructor() {
    this.locks = new Map();
  }

  /**
   * Attempts to acquire a distributed lock on a resource.
   * If the lock is held but has expired, it gets auto-released.
   * @param {string} key - The resource key to lock (e.g. 'lock:product:iphone')
   * @param {string} holderId - Unique identifier of the transaction or buyer attempting lock
   * @param {number} [ttlMs=5000] - Expiry time of the lock in milliseconds
   * @returns {boolean} True if lock acquired, false if denied
   */
  acquire(key, holderId, ttlMs = 5000) {
    const now = Date.now();
    
    if (this.locks.has(key)) {
      const lock = this.locks.get(key);
      
      // Check if lock expired
      if (now > lock.expiresAt) {
        this.locks.delete(key);
        eventBus.log('DEV', 'LOCK', `[Redlock Auto-Release] Lock expired untuk kunci: ${key}`, { key });
      } else {
        eventBus.log('DEV', 'LOCK', `[Redlock REJECT] Gagal kunci ${key} oleh ${holderId}. Lock sedang dipegang oleh ${lock.holderId}`, {
          acquired: false,
          key,
          holderId,
          heldBy: lock.holderId
        });
        return false;
      }
    }

    // Acquire lock
    this.locks.set(key, {
      holderId,
      expiresAt: now + ttlMs
    });

    eventBus.log('DEV', 'LOCK', `[Redlock ACQUIRED] Sukses mengunci ${key} untuk ${holderId} (TTL: ${ttlMs}ms)`, {
      acquired: true,
      key,
      holderId,
      ttlMs
    });
    return true;
  }

  /**
   * Releases a distributed lock if the caller is the holder.
   * @param {string} key 
   * @param {string} holderId 
   * @returns {boolean} True if released, false if mismatch or lock not found
   */
  release(key, holderId) {
    if (!this.locks.has(key)) {
      return false;
    }

    const lock = this.locks.get(key);
    if (lock.holderId === holderId) {
      this.locks.delete(key);
      eventBus.log('DEV', 'LOCK', `[Redlock RELEASE] Sukses membuka kunci ${key} oleh ${holderId}`, {
        released: true,
        key,
        holderId
      });
      return true;
    }

    eventBus.log('DEV', 'LOCK', `[Redlock RELEASE REJECT] ${holderId} gagal melepas kunci ${key} - Lock dimiliki oleh ${lock.holderId}`, {
      released: false,
      key,
      holderId
    });
    return false;
  }

  /**
   * Checks if a resource is currently locked.
   * @param {string} key 
   * @returns {boolean}
   */
  isLocked(key) {
    const now = Date.now();
    if (!this.locks.has(key)) return false;
    
    const lock = this.locks.get(key);
    if (now > lock.expiresAt) {
      this.locks.delete(key);
      return false;
    }
    return true;
  }

  /**
   * Clears all active locks.
   */
  clear() {
    this.locks.clear();
    eventBus.log('DEV', 'LOCK', 'Semua lock didistribusikan berhasil dibersihkan.');
  }

  /**
   * Exposes active locking state for visual components in Developer Mode.
   * @param {string} key 
   * @returns {object}
   */
  getLockState(key) {
    const active = this.isLocked(key);
    return {
      key,
      isLocked: active,
      holderId: active ? this.locks.get(key).holderId : null,
      expiresAt: active ? this.locks.get(key).expiresAt : null
    };
  }
}

const distributedLock = new DistributedLock();
export default distributedLock;
