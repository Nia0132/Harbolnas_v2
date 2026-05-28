import eventBus from '../eventBus.js';

class InMemoryDB {
  constructor() {
    this.initialStock = 3;
    this.reset();
  }

  /**
   * Resets the database to its initial clean state.
   * @param {number} [customStock] - Optional stock level to set.
   */
  reset(customStock = 3) {
    this.initialStock = customStock;
    this.products = {
      'iphone': {
        id: 'iphone',
        name: 'iPhone 15 Pro Max',
        price: 10000,
        originalPrice: 20000000,
        stock: customStock
      }
    };
    this.transactions = [];
    eventBus.log('DEV', 'DB', `Database di-reset. Stok awal iPhone: ${customStock} unit.`, { stock: customStock });
  }

  /**
   * Simulates querying the Read Replica (e.g. for the product catalog).
   * Adds a realistic query latency (10-15ms) to contrast with caching.
   * @param {string} productId 
   * @returns {Promise<object|null>}
   */
  async readFromReplica(productId) {
    const latency = Math.floor(Math.random() * 6) + 10; // 10ms to 15ms latency
    await new Promise(resolve => setTimeout(resolve, latency));

    const product = this.products[productId];
    if (product) {
      eventBus.log('DEV', 'DB', `[Read Replica] Query berhasil (Latency: ${latency}ms) - Stok: ${product.stock}`, {
        stock: product.stock,
        latency: `${latency}ms`
      });
      return { ...product }; // return copy to simulate db entity separation
    }
    
    eventBus.log('DEV', 'DB', `[Read Replica] Query gagal - Produk ${productId} tidak ditemukan`, { latency: `${latency}ms` });
    return null;
  }

  /**
   * Gets product directly from Primary database (for transactional consistency checks).
   * @param {string} productId 
   * @returns {object|null}
   */
  readFromPrimary(productId) {
    const product = this.products[productId];
    return product ? { ...product } : null;
  }

  /**
   * Deducts product stock in the Primary Database.
   * @param {string} productId 
   * @returns {boolean} True if successfully deducted, false if out of stock
   */
  deductStock(productId) {
    const product = this.products[productId];
    if (!product) {
      eventBus.log('DEV', 'DB', `[Primary DB] GAGAL potong stok - Produk ${productId} tidak terdaftar`);
      return false;
    }

    if (product.stock <= 0) {
      eventBus.log('DEV', 'DB', `[Primary DB] GAGAL potong stok - Produk ${product.name} habis (Stok: ${product.stock})`, { stock: product.stock });
      return false;
    }

    const prevStock = product.stock;
    product.stock -= 1;
    eventBus.log('DEV', 'DB', `[Primary DB] Sukses potong stok - ${product.name} (${prevStock} -> ${product.stock})`, {
      stock: product.stock
    });
    return true;
  }

  /**
   * Saves a transactional checkout record to the Primary Database.
   * @param {object} transaction - Transaction data.
   */
  saveTransaction(transaction) {
    const record = {
      id: `TX-${String(this.transactions.length + 1).padStart(3, '0')}`,
      timestamp: new Date().toLocaleTimeString('id-ID', { hour12: false }),
      ...transaction
    };
    this.transactions.push(record);
    eventBus.log('DEV', 'DB', `[Primary DB] Transaksi disimpan: ${record.id} untuk Pembeli: ${record.buyerName}`, {
      transactionId: record.id,
      buyerName: record.buyerName
    });
    return record;
  }

  /**
   * Returns all transactions.
   * @returns {Array}
   */
  getTransactions() {
    return [...this.transactions];
  }
}

const inMemoryDB = new InMemoryDB();
export default inMemoryDB;
