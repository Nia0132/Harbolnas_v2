import eventBus from '../eventBus.js';
import inMemoryDB from '../db/inMemoryDB.js';
import distributedLock from '../lock/distributedLock.js';
import kafkaSimulator from '../broker/kafkaSimulator.js';

class CheckoutTemplate {
  /**
   * The fixed Template Method defining the skeleton of a checkout.
   * @param {string} buyerName 
   * @param {string} productId 
   * @param {string} ip - Buyer's IP address (for rate limiting verification)
   * @returns {Promise<object>} The resulting transaction receipt
   */
  async execute(buyerName, productId, ip) {
    const lockKey = `lock:product:${productId}`;
    // Unique transaction identifier used to claim lock ownership
    const transactionToken = `TX-TOKEN-${buyerName.toUpperCase()}-${Math.floor(Math.random() * 1000)}`;

    eventBus.log('DEV', 'TEMPLATE', `[Template Method] Memulai checkout untuk ${buyerName}. Alur: Cek Stok -> Kunci -> Bayar -> DB & Kafka`, {
      buyerName,
      productId
    });

    // 1. Cek Stok (Must have stock available)
    this.cekStok(productId);

    // 2. Kunci Stok (Acquire distributed lock)
    const lockAcquired = this.kunciStok(lockKey, transactionToken);
    if (!lockAcquired) {
      const errorMsg = 'Antrean sistem padat! Gagal mengamankan stok produk.';
      eventBus.log('USER', 'TEMPLATE', `❌ Checkout DITOLAK untuk ${buyerName}: ${errorMsg}`);
      throw new Error(errorMsg);
    }

    try {
      // Re-verify stock inside the lock to prevent double-sell (Double-Checked Locking Pattern)
      const freshProduct = inMemoryDB.readFromPrimary(productId);
      if (!freshProduct || freshProduct.stock <= 0) {
        throw new Error('STOK_HABIS');
      }

      // 3. Proses Pembayaran (Abstract step overridden by subclasses)
      const paymentDetails = await this.prosesPayment(buyerName, freshProduct.price);

      // 4. Terbitkan Invoice (Commit DB, publish Kafka, release Lock)
      const tx = this.terbitkanInvoice(buyerName, productId, freshProduct.price, paymentDetails, lockKey, transactionToken);
      
      eventBus.log('USER', 'TEMPLATE', `✅ Checkout SUKSES! Invoice ${tx.id} diterbitkan untuk ${buyerName}`, {
        transactionId: tx.id,
        buyerName
      });
      return tx;

    } catch (error) {
      // Release lock on payment failures or sold-out conditions
      distributedLock.release(lockKey, transactionToken);
      
      let clientMsg = error.message;
      if (error.message === 'STOK_HABIS') {
        clientMsg = 'Maaf, produk sudah terjual habis!';
      }
      
      eventBus.log('USER', 'TEMPLATE', `❌ Checkout GAGAL untuk ${buyerName}: ${clientMsg}`);
      throw new Error(clientMsg);
    }
  }

  /**
   * Step 1: Checks if product stock exists in SQL Database.
   * @param {string} productId 
   */
  cekStok(productId) {
    const product = inMemoryDB.readFromPrimary(productId);
    if (!product || product.stock <= 0) {
      eventBus.log('DEV', 'TEMPLATE', `[Step 1: Cek Stok] Gagal - Stok ${productId} habis di DB Primary`, { stock: 0 });
      throw new Error('STOK_HABIS');
    }
    eventBus.log('DEV', 'TEMPLATE', `[Step 1: Cek Stok] Berhasil - Stok ${productId} tersedia di DB Primary: ${product.stock} unit`, { stock: product.stock });
  }

  /**
   * Step 2: Tries to acquire the Redlock distributed lock.
   * @param {string} lockKey 
   * @param {string} token 
   * @returns {boolean}
   */
  kunciStok(lockKey, token) {
    eventBus.log('DEV', 'TEMPLATE', `[Step 2: Kunci Stok] Mengajukan distributed lock untuk ${lockKey}`);
    return distributedLock.acquire(lockKey, token, 10000); // 10s lock TTL
  }

  /**
   * Step 3: Abstract payment method. Must be implemented by concrete subclasses.
   * @param {string} buyerName 
   * @param {number} amount 
   * @returns {Promise<object>}
   */
  async prosesPayment(buyerName, amount) {
    throw new Error('Metode prosesPayment() wajib di-override oleh subclass!');
  }

  /**
   * Step 4: Finalizes checkout by mutating DB Primary, publishing Kafka events, and freeing locks.
   * @param {string} buyerName 
   * @param {string} productId 
   * @param {number} amount 
   * @param {object} paymentDetails 
   * @param {string} lockKey 
   * @param {string} token 
   * @returns {object} Transaction receipt
   */
  terbitkanInvoice(buyerName, productId, amount, paymentDetails, lockKey, token) {
    eventBus.log('DEV', 'TEMPLATE', `[Step 4: Commit] Memulai penulisan transaksional DB Primary & Kafka...`);
    
    // Mutate stock in DB
    const successDeduct = inMemoryDB.deductStock(productId);
    if (!successDeduct) {
      throw new Error('STOK_HABIS');
    }

    // Save transaction
    const transaction = inMemoryDB.saveTransaction({
      buyerName,
      productId,
      amount,
      paymentMethod: paymentDetails.method,
      paymentProvider: paymentDetails.provider,
      paymentId: paymentDetails.id
    });

    // Publish asynchronous Kafka event for invoicing
    kafkaSimulator.publish('checkout-events', {
      transactionId: transaction.id,
      buyerName,
      productId,
      amount,
      paymentDetails
    });

    // Publish asynchronous Kafka event for cache invalidation
    kafkaSimulator.publish('cache-invalidation-events', {
      productId
    });

    // Release the Distributed Lock
    distributedLock.release(lockKey, token);

    return transaction;
  }
}

/**
 * Concrete Subclass: GoPay Checkout Flow
 */
export class GoPayCheckout extends CheckoutTemplate {
  async prosesPayment(buyerName, amount) {
    const latency = 60; // simulated API call to GoPay Gateway
    eventBus.log('DEV', 'TEMPLATE', `[Step 3: Bayar] GoPay Gateway: Mengirim request bayar Rp${amount.toLocaleString('id-ID')} untuk ${buyerName}`);
    
    await new Promise(resolve => setTimeout(resolve, latency));
    
    const isSuccess = Math.random() < 0.95; // 95% success rate
    if (!isSuccess) {
      eventBus.log('DEV', 'TEMPLATE', `[Step 3: Bayar] GoPay Gateway: Gagal memproses saldo / limit untuk ${buyerName}`);
      throw new Error('Saldo GoPay tidak mencukupi atau limit habis.');
    }

    eventBus.log('DEV', 'TEMPLATE', `[Step 3: Bayar] GoPay Gateway: Sukses diproses (Latency: ${latency}ms)`);
    return {
      method: 'E-Wallet',
      provider: 'GoPay',
      id: `GPY-${Math.floor(100000 + Math.random() * 900000)}`
    };
  }
}

/**
 * Concrete Subclass: Bank Transfer / Virtual Account Checkout Flow
 */
export class BankTransferCheckout extends CheckoutTemplate {
  async prosesPayment(buyerName, amount) {
    const latency = 120; // Simulated network delay for banking core
    eventBus.log('DEV', 'TEMPLATE', `[Step 3: Bayar] Core Banking Virtual Account: Verifikasi dana Rp${amount.toLocaleString('id-ID')} untuk ${buyerName}`);
    
    await new Promise(resolve => setTimeout(resolve, latency));
    
    eventBus.log('DEV', 'TEMPLATE', `[Step 3: Bayar] Core Banking Virtual Account: Pembayaran berhasil terverifikasi (Latency: ${latency}ms)`);
    return {
      method: 'Bank Transfer',
      provider: 'VA BNI / Mandiri',
      id: `TRF-${Math.floor(100000 + Math.random() * 900000)}`
    };
  }
}
