import eventBus from '../eventBus.js';

class KafkaSimulator {
  constructor() {
    this.queue = [];
    this.active = false;
    this.intervalId = null;
  }

  /**
   * Publishes an event to a Kafka topic.
   * @param {string} topic - Topic name (e.g. 'checkout-success' or 'cache-invalidation')
   * @param {object} message - Event message body
   */
  publish(topic, message) {
    const event = {
      offset: this.queue.length + 1,
      timestamp: Date.now(),
      topic,
      data: message
    };
    this.queue.push(event);

    eventBus.log('DEV', 'KAFKA', `[Kafka Producer] Event dipublikasikan ke topik '${topic}' (Offset: ${event.offset})`, {
      topic,
      offset: event.offset,
      eventData: message
    });
  }

  /**
   * Starts the Kafka background consumer loop.
   * Processes exactly one message every 1 second (simulating async pipeline).
   * @param {function} onMessageCallback - Fn invoked with the message {topic, data}
   */
  startConsumer(onMessageCallback) {
    if (this.active) return;
    this.active = true;
    eventBus.log('DEV', 'KAFKA', 'Kafka Consumer Loop AKTIF. Mendengarkan topik...');

    const processNext = async () => {
      if (!this.active) return;

      if (this.queue.length > 0) {
        const event = this.queue.shift(); // FIFO order
        eventBus.log('DEV', 'KAFKA', `[Kafka Consumer] Mengonsumsi event dari '${event.topic}' (Offset: ${event.offset})`, {
          topic: event.topic,
          offset: event.offset
        });
        
        try {
          await onMessageCallback(event);
        } catch (err) {
          eventBus.log('DEV', 'KAFKA', `[Kafka Consumer ERROR] Gagal memproses event offset: ${event.offset}. Error: ${err.message}`);
        }
      }

      this.intervalId = setTimeout(processNext, 1000); // schedule next tick
    };

    this.intervalId = setTimeout(processNext, 1000);
  }

  /**
   * Stops the background consumer.
   */
  stopConsumer() {
    this.active = false;
    if (this.intervalId) {
      clearTimeout(this.intervalId);
      this.intervalId = null;
    }
    eventBus.log('DEV', 'KAFKA', 'Kafka Consumer Loop DIMATIKAN.');
  }

  /**
   * Clears all pending events in the queue.
   */
  clear() {
    this.queue = [];
    eventBus.log('DEV', 'KAFKA', 'Antrean Kafka berhasil dibersihkan.');
  }

  /**
   * Gets pending messages in the queue for visual rendering.
   * @returns {Array}
   */
  getQueueState() {
    return [...this.queue];
  }
}

const kafkaSimulator = new KafkaSimulator();
export default kafkaSimulator;
