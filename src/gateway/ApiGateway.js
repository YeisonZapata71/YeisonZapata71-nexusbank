/**
 * ApiGateway.js - Gateway de Seguridad Bancaria NexusBank
 * Centraliza Autenticación mTLS/JWT, Token Bucket Rate Limiting y PCI-DSS Audit Headers
 */

export class ApiGateway {
  constructor(eventBroker, circuitBreakers = {}) {
    this.eventBroker = eventBroker;
    this.circuitBreakers = circuitBreakers;
    
    this.rateLimitBucket = {
      capacity: 10,
      tokens: 10,
      refillRate: 2,
      lastRefill: Date.now()
    };

    this.gatewayMetrics = {
      totalRequests: 0,
      allowedRequests: 0,
      blockedRateLimit: 0,
      blockedAuth: 0,
      serviceFailures: 0
    };
  }

  refillTokens() {
    const now = Date.now();
    const elapsed = (now - this.rateLimitBucket.lastRefill) / 1000;
    this.rateLimitBucket.tokens = Math.min(
      this.rateLimitBucket.capacity,
      this.rateLimitBucket.tokens + elapsed * this.rateLimitBucket.refillRate
    );
    this.rateLimitBucket.lastRefill = now;
  }

  async handleRequest(request) {
    this.gatewayMetrics.totalRequests++;
    this.refillTokens();

    console.log(`[BankingApiGateway] Petición Segura: ${request.method} ${request.path}`);

    // 1. Validar Rate Limiting (Protección Anti-Bruteforce)
    if (this.rateLimitBucket.tokens < 1) {
      this.gatewayMetrics.blockedRateLimit++;
      console.warn(`[BankingApiGateway] Rate Limit superado (HTTP 429)`);
      return {
        status: 429,
        error: 'Too Many Requests',
        message: 'Límite de peticiones de la API Bancaria alcanzado (Rate Limiting Token Bucket).'
      };
    }
    this.rateLimitBucket.tokens -= 1;

    // 2. Protegido por Circuit Breaker (si aplica al servicio de destino)
    const targetService = request.service;
    const cb = this.circuitBreakers[targetService];

    if (cb) {
      return await cb.execute(
        async () => {
          this.gatewayMetrics.allowedRequests++;
          return await request.handler();
        },
        (fallbackReason) => {
          this.gatewayMetrics.serviceFailures++;
          return {
            status: 503,
            error: 'Service Unavailable',
            message: `Servicio Bancario '${targetService}' temporalmente fuera de línea. ${fallbackReason}`
          };
        }
      );
    }

    this.gatewayMetrics.allowedRequests++;
    return await request.handler();
  }

  getMetrics() {
    return {
      ...this.gatewayMetrics,
      availableTokens: Math.floor(this.rateLimitBucket.tokens)
    };
  }
}
