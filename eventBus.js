import { EventEmitter } from 'events';

class AppEventBus extends EventEmitter {
  /**
   * Dispatches a structured log event to both the console and connected web clients.
   * @param {'USER' | 'DEV'} type - The category of the event. 'USER' is shopper-facing, 'DEV' is system-internal.
   * @param {'GATEWAY' | 'REDIS' | 'DB' | 'LOCK' | 'KAFKA' | 'TEMPLATE' | 'SYSTEM'} component - The microservice or component emitting the event.
   * @param {string} message - Descriptive text log.
   * @param {object} [metadata] - Optional additional keys (e.g. stock, invoiceId, isHit).
   */
  log(type, component, message, metadata = {}) {
    const timestamp = new Date().toLocaleTimeString('id-ID', { hour12: false });
    this.emit('log', {
      timestamp,
      type,
      component,
      message,
      metadata
    });
  }
}

const eventBus = new AppEventBus();
export default eventBus;
