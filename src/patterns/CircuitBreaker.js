/**
 * CircuitBreaker.js - Patrón Circuit Breaker Financiero
 * Protege las llamadas al Servicio de Detección de Fraude / Redes Interbancarias
 */

export const CircuitState = {
  CLOSED: 'CLOSED',      // Funcionamiento normal (Pasan las transacciones)
  OPEN: 'OPEN',          // Circuito Abierto (Rechazo rápido / Fallback de riesgo)
  HALF_OPEN: 'HALF_OPEN' // Modo prueba de recuperación
};

export class CircuitBreaker {
  constructor(serviceName, options = {}) {
    this.serviceName = serviceName;
    this.failureThreshold = options.failureThreshold || 3;
    this.resetTimeoutMs = options.resetTimeoutMs || 6000;
    this.halfOpenSuccessThreshold = options.halfOpenSuccessThreshold || 2;

    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    this.successCountInHalfOpen = 0;
    this.lastStateChange = Date.now();
    this.metrics = {
      totalRequests: 0,
      totalSuccesses: 0,
      totalFailures: 0,
      rejectedRequests: 0
    };

    this.onStateChangeCallbacks = [];
  }

  async execute(action, fallback) {
    this.metrics.totalRequests++;

    if (this.state === CircuitState.OPEN) {
      const now = Date.now();
      if (now - this.lastStateChange >= this.resetTimeoutMs) {
        this.transitionTo(CircuitState.HALF_OPEN);
      } else {
        this.metrics.rejectedRequests++;
        console.warn(`[CircuitBreaker:${this.serviceName}] Circuito ABIERTO. Invocando evaluación de riesgo contingente.`);
        if (fallback) return fallback('Circuit Breaker OPEN - Fast Fail Antifraude');
        throw new Error(`[CircuitBreaker] Servicio '${this.serviceName}' NO disponible.`);
      }
    }

    try {
      const result = await action();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure(error);
      if (fallback) return fallback(error.message);
      throw error;
    }
  }

  onSuccess() {
    this.metrics.totalSuccesses++;
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCountInHalfOpen++;
      if (this.successCountInHalfOpen >= this.halfOpenSuccessThreshold) {
        this.transitionTo(CircuitState.CLOSED);
      }
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount = 0;
    }
  }

  onFailure(error) {
    this.metrics.totalFailures++;
    this.failureCount++;
    if (this.state === CircuitState.HALF_OPEN || this.failureCount >= this.failureThreshold) {
      this.transitionTo(CircuitState.OPEN);
    }
  }

  transitionTo(newState) {
    const oldState = this.state;
    this.state = newState;
    this.lastStateChange = Date.now();

    if (newState === CircuitState.CLOSED) {
      this.failureCount = 0;
      this.successCountInHalfOpen = 0;
    } else if (newState === CircuitState.HALF_OPEN) {
      this.successCountInHalfOpen = 0;
    }

    this.onStateChangeCallbacks.forEach(cb => cb(this.serviceName, newState, oldState, this.getStats()));
  }

  onStateChange(callback) {
    this.onStateChangeCallbacks.push(callback);
  }

  getStats() {
    return {
      serviceName: this.serviceName,
      state: this.state,
      failureCount: this.failureCount,
      failureThreshold: this.failureThreshold,
      metrics: { ...this.metrics }
    };
  }

  forceReset() {
    this.transitionTo(CircuitState.CLOSED);
  }
}
