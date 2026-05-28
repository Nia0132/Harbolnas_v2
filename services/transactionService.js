import eventBus from '../eventBus.js';
import { GoPayCheckout, BankTransferCheckout } from '../patterns/checkoutTemplate.js';

class TransactionService {
  constructor() {
    this.gopayTemplate = new GoPayCheckout();
    this.bankTemplate = new BankTransferCheckout();
  }

  /**
   * Orchestrates the checkout process by invoking the proper Checkout Template Method.
   * @param {string} buyerName 
   * @param {string} productId 
   * @param {'gopay' | 'bank_transfer'} paymentMethod 
   * @param {string} ip - Buyer's IP address (for logging/security tracing)
   * @returns {Promise<object>} Transaction receipt
   */
  async checkout(buyerName, productId, paymentMethod, ip) {
    eventBus.log('DEV', 'SYSTEM', `[Transaction Service] Request POST /checkout/${productId} diterima dari ${buyerName} (${ip})`);

    let template;
    if (paymentMethod === 'gopay') {
      template = this.gopayTemplate;
    } else if (paymentMethod === 'bank_transfer') {
      template = this.bankTemplate;
    } else {
      const errorMsg = `Metode pembayaran '${paymentMethod}' tidak didukung.`;
      eventBus.log('DEV', 'SYSTEM', `[Transaction Service] GAGAL: ${errorMsg}`);
      throw new Error(errorMsg);
    }

    // Execute the rigid checkout template method
    return await template.execute(buyerName, productId, ip);
  }
}

const transactionService = new TransactionService();
export default transactionService;
