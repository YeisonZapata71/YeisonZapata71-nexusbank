/**
 * EventBroker.js - Bus de Eventos Financieros (Pub/Sub)
 * Simula Apache Kafka / AWS EventBridge para NexusBank Core
 */

export class EventBroker {
  constructor() {
    this.topics = new Map();
    this.eventHistory = [];
    this.subscribers = new Map();
    this.onEventCallbacks = [];
  }

  subscribe(topic, serviceName, callback) {
    if (!this.topics.has(topic)) {
      this.topics.set(topic, []);
    }
    this.topics.get(topic).push({ serviceName, callback });

    if (!this.subscribers.has(serviceName)) {
      this.subscribers.set(serviceName, []);
    }
    this.subscribers.get(serviceName).push(topic);
  }

  publish(topic, payload, sourceService = 'BankingCore') {
    const event = {
      id: `EVT-BANK-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      topic,
      source: sourceService,
      timestamp: new Date().toISOString(),
      payload
    };

    this.eventHistory.unshift(event);
    if (this.eventHistory.length > 100) this.eventHistory.pop();

    const listeners = this.topics.get(topic) || [];
    listeners.forEach(({ serviceName, callback }) => {
      setTimeout(() => {
        try {
          callback(event);
        } catch (error) {
          console.error(`[EventBroker] Error en ${serviceName}:`, error);
        }
      }, 40); // Latencia simulada de bus bancario (40ms)
    });

    this.onEventCallbacks.forEach(cb => cb(event));
    return event;
  }

  onGlobalEvent(callback) {
    this.onEventCallbacks.push(callback);
  }

  getHistory() {
    return this.eventHistory;
  }
}
