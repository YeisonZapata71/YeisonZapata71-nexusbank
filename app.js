/**
 * app.js - Prototipo Funcional Standalone para NexusBank Core
 * Diseñado en Vanilla JavaScript para ejecutar sin restricciones de CORS (funciona con doble clic en file:// o en servidor HTTP)
 * Moneda: Pesos Colombianos (COP)
 */

(function () {
  'use strict';

  // ==========================================
  // 1. EVENT BROKER (Bus Pub/Sub Financiero)
  // ==========================================
  class EventBroker {
    constructor() {
      this.topics = new Map();
      this.eventHistory = [];
      this.subscribers = new Map();
      this.onEventCallbacks = [];
    }

    subscribe(topic, serviceName, callback) {
      if (!this.topics.has(topic)) this.topics.set(topic, []);
      this.topics.get(topic).push({ serviceName, callback });

      if (!this.subscribers.has(serviceName)) this.subscribers.set(serviceName, []);
      this.subscribers.get(serviceName).push(topic);
    }

    publish(topic, payload, sourceService = 'NexusBankCore') {
      const event = {
        id: `EVT-COP-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
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
          } catch (err) {
            console.error(`[EventBroker] Error en ${serviceName}:`, err);
          }
        }, 30);
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

  // ==========================================
  // 2. CIRCUIT BREAKER (Resiliencia Antifraude)
  // ==========================================
  const CircuitState = {
    CLOSED: 'CLOSED',
    OPEN: 'OPEN',
    HALF_OPEN: 'HALF_OPEN'
  };

  class CircuitBreaker {
    constructor(serviceName, options = {}) {
      this.serviceName = serviceName;
      this.failureThreshold = options.failureThreshold || 3;
      this.resetTimeoutMs = options.resetTimeoutMs || 6000;
      this.halfOpenSuccessThreshold = options.halfOpenSuccessThreshold || 2;

      this.state = CircuitState.CLOSED;
      this.failureCount = 0;
      this.successCountInHalfOpen = 0;
      this.lastStateChange = Date.now();
      this.metrics = { totalRequests: 0, totalSuccesses: 0, totalFailures: 0, rejectedRequests: 0 };
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
          console.warn(`[CircuitBreaker:${this.serviceName}] Circuito ABIERTO. Invocando evaluación de contingencia.`);
          if (fallback) return fallback('Circuit Breaker OPEN - Fast Fail Antifraude');
          throw new Error(`Servicio ${this.serviceName} no disponible (Circuit OPEN).`);
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

    onStateChange(cb) { this.onStateChangeCallbacks.push(cb); }
    getStats() { return { serviceName: this.serviceName, state: this.state, failureCount: this.failureCount, failureThreshold: this.failureThreshold, metrics: { ...this.metrics } }; }
    forceReset() { this.transitionTo(CircuitState.CLOSED); }
  }

  // ==========================================
  // 3. CQRS STORES (Libro Mayor & Vistas Redis)
  // ==========================================
  class WriteCommandStore {
    constructor(eventBroker) {
      this.eventBroker = eventBroker;
      this.writeLedgerDb = new Map();
    }

    executeCommand(commandName, payload) {
      const commandId = `CMD-COP-${Date.now()}`;
      switch (commandName) {
        case 'ExecuteTransfer':
          const tx = {
            id: payload.transferId || `TRF-COP-${Math.floor(100000 + Math.random() * 900000)}`,
            sourceAccount: payload.sourceAccount,
            targetAccount: payload.targetAccount,
            amount: payload.amount,
            status: 'COMPLETED',
            timestamp: new Date().toISOString()
          };
          this.writeLedgerDb.set(tx.id, tx);
          this.eventBroker.publish('transfer.executed', tx, 'CQRS-WriteLedger');
          return { success: true, commandId, data: tx };

        case 'RollbackTransfer':
          if (this.writeLedgerDb.has(payload.transferId)) {
            const txR = this.writeLedgerDb.get(payload.transferId);
            txR.status = 'RECONCILIADA_REEMBOLSADA';
            this.writeLedgerDb.set(txR.id, txR);
            this.eventBroker.publish('transfer.reversed', txR, 'CQRS-WriteLedger');
            return { success: true, commandId, data: txR };
          }
          return { success: false, error: 'Transacción no encontrada' };

        default:
          throw new Error(`Comando ${commandName} desconocido.`);
      }
    }
  }

  class ReadQueryStore {
    constructor(eventBroker) {
      this.eventBroker = eventBroker;
      this.materializedStatementView = new Map();

      this.eventBroker.subscribe('transfer.executed', 'CQRS-ReadProjection', (evt) => {
        this.projectTransferCreated(evt.payload);
      });

      this.eventBroker.subscribe('transfer.reversed', 'CQRS-ReadProjection', (evt) => {
        this.projectTransferReversed(evt.payload);
      });
    }

    projectTransferCreated(tx) {
      const projection = {
        id: tx.id,
        from: tx.sourceAccount,
        to: tx.targetAccount,
        formattedAmount: `$ ${tx.amount.toLocaleString('es-CO')} COP`,
        status: tx.status,
        timestamp: new Date(tx.timestamp).toLocaleTimeString()
      };
      this.materializedStatementView.set(tx.id, projection);
    }

    projectTransferReversed(tx) {
      if (this.materializedStatementView.has(tx.id)) {
        const proj = this.materializedStatementView.get(tx.id);
        proj.status = 'RECONCILIADA_REEMBOLSADA';
        this.materializedStatementView.set(tx.id, proj);
      }
    }

    queryStatementHistory() {
      return Array.from(this.materializedStatementView.values());
    }
  }

  // ==========================================
  // 4. SAGA ORCHESTRATOR (Transacciones COP)
  // ==========================================
  class SagaOrchestrator {
    constructor(services, eventBroker) {
      this.services = services;
      this.eventBroker = eventBroker;
      this.sagaLogs = [];
    }

    async executeInterbankTransferSaga(transferData) {
      const sagaId = `SAGA-COP-${Date.now()}`;
      const formattedMonto = `$ ${transferData.amount.toLocaleString('es-CO')} COP`;
      const log = {
        sagaId,
        transferId: transferData.transferId,
        stepsCompleted: [],
        status: 'IN_PROGRESS',
        startTime: new Date().toISOString(),
        details: []
      };
      this.sagaLogs.unshift(log);

      this.recordLog(log, `Paso 1: Iniciando validación para cuenta origen ${transferData.sourceAccount}`);

      try {
        // Paso 1: Débito
        this.recordLog(log, `Paso 1: Debitando ${formattedMonto} de la cuenta origen ${transferData.sourceAccount}...`);
        const debitRes = await this.services.accountService.debitAccount(transferData.sourceAccount, transferData.amount);
        if (!debitRes.success) throw new Error(`Saldo insuficiente o cuenta bloqueada: ${debitRes.reason}`);
        log.stepsCompleted.push({ step: 'DEBIT_SENDER', rollbackData: { account: transferData.sourceAccount, amount: transferData.amount } });
        this.recordLog(log, `✓ Débito exitoso. Nuevo saldo en cuenta: $ ${debitRes.newBalance.toLocaleString('es-CO')} COP`);

        // Paso 2: Evaluación Antifraude
        this.recordLog(log, `Paso 2: Evaluando riesgo de fraude en FraudDetectionService...`);
        const fraudRes = await this.services.fraudService.evaluateRisk(transferData.sourceAccount, transferData.amount);
        if (!fraudRes.approved) throw new Error(`Alerta de Fraude: Transacción bloqueada por política de seguridad (${fraudRes.reason})`);
        log.stepsCompleted.push({ step: 'FRAUD_CHECK', rollbackData: null });
        this.recordLog(log, `✓ Antifraude APROBADO (Score de Riesgo: ${fraudRes.riskScore}/100)`);

        // Paso 3: Crédito Destino
        this.recordLog(log, `Paso 3: Acreditando ${formattedMonto} a la cuenta destino ${transferData.targetAccount}...`);
        const creditRes = await this.services.accountService.creditAccount(transferData.targetAccount, transferData.amount);
        if (!creditRes.success) throw new Error(`Banco destino no responde o cuenta inválida: ${creditRes.reason}`);
        log.stepsCompleted.push({ step: 'CREDIT_RECEIVER', rollbackData: { account: transferData.targetAccount, amount: transferData.amount } });
        this.recordLog(log, `✓ Crédito interbancario exitoso en cuenta ${transferData.targetAccount}`);

        log.status = 'COMPLETED';
        log.endTime = new Date().toISOString();
        this.recordLog(log, `🎉 TRANSFERENCIA EXITOSA. Comprobante registrado en el Libro Mayor.`);

        this.eventBroker.publish('transfer.saga_completed', { sagaId, transferId: transferData.transferId, amount: transferData.amount }, 'SagaOrchestrator');
        return { success: true, sagaId, log };

      } catch (error) {
        this.recordLog(log, `❌ RECHAZO EN SAGA BANCARIA: ${error.message}. Iniciando Reversión Compensatoria LIFO...`);
        log.status = 'FAILED_COMPENSATING';
        await this.rollbackSaga(log);
        log.status = 'COMPENSATED';
        log.endTime = new Date().toISOString();
        this.eventBroker.publish('transfer.saga_compensated', { sagaId, transferId: transferData.transferId, reason: error.message }, 'SagaOrchestrator');
        return { success: false, sagaId, error: error.message, log };
      }
    }

    async rollbackSaga(log) {
      const stepsToRollback = [...log.stepsCompleted].reverse();
      for (const stepInfo of stepsToRollback) {
        switch (stepInfo.step) {
          case 'CREDIT_RECEIVER':
            this.recordLog(log, ` [Compensación] Reversando abono en cuenta destino ${stepInfo.rollbackData.account}...`);
            await this.services.accountService.debitAccount(stepInfo.rollbackData.account, stepInfo.rollbackData.amount);
            break;
          case 'DEBIT_SENDER':
            const montoComp = `$ ${stepInfo.rollbackData.amount.toLocaleString('es-CO')} COP`;
            this.recordLog(log, ` [Compensación] REEMBOLSANDO ${montoComp} a la cuenta origen ${stepInfo.rollbackData.account}...`);
            await this.services.accountService.creditAccount(stepInfo.rollbackData.account, stepInfo.rollbackData.amount);
            break;
        }
      }
      this.recordLog(log, `✓ Reversión compensatoria completada. Fondos restituidos al cliente de origen.`);
    }

    recordLog(logObj, message) {
      logObj.details.push(`[${new Date().toLocaleTimeString()}] ${message}`);
    }

    getLogs() { return this.sagaLogs; }
  }

  // ==========================================
  // 5. API GATEWAY (Rate Limiting & Auth)
  // ==========================================
  class ApiGateway {
    constructor(eventBroker, circuitBreakers = {}) {
      this.eventBroker = eventBroker;
      this.circuitBreakers = circuitBreakers;
      this.rateLimitBucket = { capacity: 10, tokens: 10, refillRate: 2, lastRefill: Date.now() };
      this.gatewayMetrics = { totalRequests: 0, allowedRequests: 0, blockedRateLimit: 0, blockedAuth: 0, serviceFailures: 0 };
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

      if (this.rateLimitBucket.tokens < 1) {
        this.gatewayMetrics.blockedRateLimit++;
        return {
          status: 429,
          error: 'Too Many Requests',
          message: 'Límite de peticiones de la API Bancaria alcanzado (Rate Limiting Token Bucket).'
        };
      }
      this.rateLimitBucket.tokens -= 1;

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
              message: `Servicio '${targetService}' fuera de línea. ${fallbackReason}`
            };
          }
        );
      }

      this.gatewayMetrics.allowedRequests++;
      return await request.handler();
    }

    getMetrics() {
      return { ...this.gatewayMetrics, availableTokens: Math.floor(this.rateLimitBucket.tokens) };
    }
  }

  // ==========================================
  // 6. MICROSERVICIOS BANCARIOS (COP)
  // ==========================================
  class AccountService {
    constructor(eventBroker) {
      this.eventBroker = eventBroker;
      this.accounts = new Map([
        ['ACC-9988-COLL', { owner: 'Juan Pérez (Cuenta Nómina)', balance: 12850000, currency: 'COP' }],
        ['ACC-5544-SAVE', { owner: 'María Rodríguez (Cuenta Ahorros)', balance: 5400000, currency: 'COP' }],
        ['ACC-1122-DEST', { owner: 'Carlos Gómez (Cuenta Destino)', balance: 1200000, currency: 'COP' }]
      ]);
    }

    async debitAccount(accountNum, amount) {
      const acc = this.accounts.get(accountNum);
      if (!acc) return { success: false, reason: 'Cuenta origen no existe' };
      if (acc.balance < amount) return { success: false, reason: `Saldo insuficiente ($ ${acc.balance.toLocaleString('es-CO')} COP disponibles)` };

      acc.balance -= amount;
      this.eventBroker.publish('account.debited', { accountNum, amount, newBalance: acc.balance }, 'AccountService');
      return { success: true, newBalance: acc.balance };
    }

    async creditAccount(accountNum, amount) {
      const acc = this.accounts.get(accountNum);
      if (!acc) return { success: false, reason: 'Cuenta destino no existe' };

      acc.balance += amount;
      this.eventBroker.publish('account.credited', { accountNum, amount, newBalance: acc.balance }, 'AccountService');
      return { success: true, newBalance: acc.balance };
    }

    getAccountsState() {
      return Array.from(this.accounts.entries()).map(([num, data]) => ({ num, ...data }));
    }
  }

  class FraudDetectionService {
    constructor(eventBroker) {
      this.eventBroker = eventBroker;
      this.shouldFailNetwork = false;
    }

    setNetworkFault(failState) {
      this.shouldFailNetwork = failState;
    }

    async evaluateRisk(accountNum, amount) {
      if (this.shouldFailNetwork) {
        throw new Error('503 Service Unavailable: Motor Antifraude fuera de línea (Timeout de Red)');
      }

      if (amount > 20000000) {
        return { approved: false, riskScore: 95, reason: 'Monto excede tope transaccional de seguridad sin confirmación biométrica' };
      }

      const riskScore = Math.floor(5 + Math.random() * 15);
      return { approved: true, riskScore, reason: 'Riesgo Bajo' };
    }
  }

  class NotificationService {
    constructor(eventBroker) {
      this.eventBroker = eventBroker;
      this.notificationsLog = [];

      this.eventBroker.subscribe('transfer.saga_completed', 'NotificationService', (evt) => {
        const monto = `$ ${evt.payload.amount.toLocaleString('es-CO')} COP`;
        this.sendNotification(`📲 SMS/Push NexusBank: Transferencia por ${monto} REALIZADA CON ÉXITO. Ref: ${evt.payload.transferId}`);
      });

      this.eventBroker.subscribe('transfer.saga_compensated', 'NotificationService', (evt) => {
        this.sendNotification(`🚨 Alerta NexusBank: Transferencia DENEGADA. Se realizó la devolución del dinero a su cuenta. Motivo: ${evt.payload.reason}`);
      });
    }

    sendNotification(message) {
      const notif = { id: `SMS-${Date.now()}`, message, timestamp: new Date().toLocaleTimeString() };
      this.notificationsLog.unshift(notif);
    }

    getNotifications() { return this.notificationsLog; }
  }

  // ==========================================
  // 7. DASHBOARD UI MANAGER
  // ==========================================
  class DashboardManager {
    constructor(appContext) {
      this.app = appContext;
      this.initEventListeners();
      this.initGlobalEventSubscribers();
      this.renderAll();

      setInterval(() => this.updateMetricsUI(), 1000);
    }

    initEventListeners() {
      document.getElementById('btn-execute-transfer')?.addEventListener('click', () => {
        this.handleExecuteTransfer();
      });

      document.getElementById('btn-inject-fraud-fault')?.addEventListener('click', () => {
        const btn = document.getElementById('btn-inject-fraud-fault');
        const isActive = btn.classList.toggle('active-fault');
        this.app.fraudService.setNetworkFault(isActive);
        btn.innerText = isActive ? '🔥 Caída Motor Antifraude: ACTIVADO (503 Error)' : '⚡ Inyectar Caída en Motor Antifraude';
        btn.style.background = isActive ? '#d93025' : '#ea4335';
        btn.style.color = '#ffffff';
      });

      document.getElementById('btn-rate-limit-spike')?.addEventListener('click', () => {
        this.triggerRateLimitSpike();
      });

      document.getElementById('btn-reset-cb')?.addEventListener('click', () => {
        this.app.fraudCircuitBreaker.forceReset();
        this.renderCircuitBreakerUI();
      });

      document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
          document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
          document.querySelectorAll('.diagram-pane').forEach(p => p.classList.remove('active'));
          e.target.classList.add('active');
          const targetPane = document.getElementById(`pane-${e.target.dataset.tab}`);
          if (targetPane) targetPane.classList.add('active');
        });
      });
    }

    initGlobalEventSubscribers() {
      this.app.eventBroker.onGlobalEvent((event) => {
        this.renderEventLog(event);
        this.renderCQRSViews();
        this.renderAccountsTable();
      });

      this.app.fraudCircuitBreaker.onStateChange(() => {
        this.renderCircuitBreakerUI();
      });
    }

    async handleExecuteTransfer() {
      const sourceAccount = document.getElementById('select-source-acc').value;
      const targetAccount = document.getElementById('select-target-acc').value;
      const amount = parseFloat(document.getElementById('input-amount').value) || 250000;

      if (sourceAccount === targetAccount) {
        alert('La cuenta de origen y destino deben ser diferentes.');
        return;
      }

      const transferData = {
        transferId: `TRF-COP-${Math.floor(100000 + Math.random() * 900000)}`,
        sourceAccount,
        targetAccount,
        amount
      };

      const gatewayRequest = {
        method: 'POST',
        path: '/api/v1/transfers/interbank',
        service: 'FraudDetectionService',
        handler: async () => {
          this.app.writeStore.executeCommand('ExecuteTransfer', transferData);
          return await this.app.sagaOrchestrator.executeInterbankTransferSaga(transferData);
        }
      };

      const response = await this.app.apiGateway.handleRequest(gatewayRequest);
      this.renderSagaLog();
      this.renderNotifications();
      this.updateMetricsUI();

      if (response.status && response.status >= 400) {
        alert(`[NexusBank Security Alert] HTTP ${response.status}: ${response.message}`);
      }
    }

    async triggerRateLimitSpike() {
      for (let i = 1; i <= 12; i++) {
        const req = {
          method: 'GET',
          path: '/api/v1/accounts/balance',
          service: 'AccountService',
          handler: async () => ({ success: true, reqNum: i })
        };
        await this.app.apiGateway.handleRequest(req);
      }
      this.updateMetricsUI();
    }

    renderAll() {
      this.renderCircuitBreakerUI();
      this.renderAccountsTable();
      this.renderCQRSViews();
      this.renderNotifications();
    }

    renderCircuitBreakerUI() {
      const cb = this.app.fraudCircuitBreaker;
      const stats = cb.getStats();
      const badge = document.getElementById('cb-state-badge');
      const failureCountEl = document.getElementById('cb-failure-count');
      const totalReqEl = document.getElementById('cb-total-req');
      const rejectedReqEl = document.getElementById('cb-rejected-req');

      if (badge) {
        badge.className = `status-badge state-${stats.state.toLowerCase()}`;
        badge.innerText = stats.state;
      }
      if (failureCountEl) failureCountEl.innerText = `${stats.failureCount} / ${stats.failureThreshold}`;
      if (totalReqEl) totalReqEl.innerText = stats.metrics.totalRequests;
      if (rejectedReqEl) rejectedReqEl.innerText = stats.metrics.rejectedRequests;
    }

    renderEventLog(event) {
      const logContainer = document.getElementById('event-stream-log');
      if (!logContainer) return;

      const item = document.createElement('div');
      item.className = 'event-log-item';
      item.innerHTML = `
        <div class="event-header">
          <span class="event-topic">${event.topic}</span>
          <span class="event-time">${new Date(event.timestamp).toLocaleTimeString()}</span>
        </div>
        <div class="event-body">
          <span class="event-source">Origen: <strong>${event.source}</strong></span>
          <pre>${JSON.stringify(event.payload, null, 2)}</pre>
        </div>
      `;
      logContainer.insertBefore(item, logContainer.firstChild);
    }

    renderSagaLog() {
      const sagaContainer = document.getElementById('saga-execution-log');
      if (!sagaContainer) return;

      const logs = this.app.sagaOrchestrator.getLogs();
      if (logs.length === 0) return;

      const latest = logs[0];
      const statusClass = latest.status === 'COMPLETED' ? 'saga-success' : 'saga-failed';
      let stepsHtml = latest.details.map(d => `<li>${d}</li>`).join('');

      sagaContainer.innerHTML = `
        <div class="saga-card ${statusClass}">
          <div class="saga-header">
            <strong>Saga Transaccional: ${latest.sagaId}</strong>
            <span class="saga-badge">${latest.status}</span>
          </div>
          <ul class="saga-steps">
            ${stepsHtml}
          </ul>
        </div>
      `;
    }

    renderCQRSViews() {
      const readViewContainer = document.getElementById('cqrs-read-table');
      if (!readViewContainer) return;

      const readModels = this.app.readQueryStore.queryStatementHistory();
      if (readModels.length === 0) {
        readViewContainer.innerHTML = '<p class="empty-msg" style="color:#5f6368; font-size:13px;">No hay transferencias recientes en la vista de lectura bancaria.</p>';
        return;
      }

      let rows = readModels.map(m => `
        <tr>
          <td><code>${m.id}</code></td>
          <td>${m.from} ➔ ${m.to}</td>
          <td><strong>${m.formattedAmount}</strong></td>
          <td><span class="order-status status-${m.status.toLowerCase()}">${m.status}</span></td>
          <td>${m.timestamp}</td>
        </tr>
      `).join('');

      readViewContainer.innerHTML = `
        <table class="data-table">
          <thead>
            <tr>
              <th>ID Transacción</th>
              <th>Origen / Destino</th>
              <th>Monto (COP)</th>
              <th>Estado (CQRS Cache)</th>
              <th>Hora</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }

    renderAccountsTable() {
      const container = document.getElementById('accounts-table-body');
      if (!container) return;

      const accounts = this.app.accountService.getAccountsState();
      container.innerHTML = accounts.map(a => `
        <tr>
          <td><code>${a.num}</code></td>
          <td>${a.owner}</td>
          <td><strong style="color:#1a73e8;">$ ${a.balance.toLocaleString('es-CO')} ${a.currency}</strong></td>
        </tr>
      `).join('');
    }

    renderNotifications() {
      const container = document.getElementById('notification-log');
      if (!container) return;

      const notifs = this.app.notificationService.getNotifications();
      container.innerHTML = notifs.slice(0, 5).map(n => `
        <div class="notif-item">
          <span class="notif-time">${n.timestamp}</span>
          <span class="notif-msg">${n.message}</span>
        </div>
      `).join('');
    }

    updateMetricsUI() {
      const metrics = this.app.apiGateway.getMetrics();
      const tokensEl = document.getElementById('gw-available-tokens');
      const reqEl = document.getElementById('gw-total-req');
      const blockedEl = document.getElementById('gw-blocked-rate');

      if (tokensEl) tokensEl.innerText = `${metrics.availableTokens} / 10`;
      if (reqEl) reqEl.innerText = metrics.totalRequests;
      if (blockedEl) blockedEl.innerText = metrics.blockedRateLimit;
    }
  }

  // ==========================================
  // 8. BOOTSTRAPPER (Arranque de la App)
  // ==========================================
  class NexusBankApp {
    constructor() {
      console.log('🏦 Inicializando Plataforma Bancaria NexusBank Core (COP)...');
      this.eventBroker = new EventBroker();
      this.accountService = new AccountService(this.eventBroker);
      this.fraudService = new FraudDetectionService(this.eventBroker);
      this.notificationService = new NotificationService(this.eventBroker);
      this.writeStore = new WriteCommandStore(this.eventBroker);
      this.readQueryStore = new ReadQueryStore(this.eventBroker);

      this.fraudCircuitBreaker = new CircuitBreaker('FraudDetectionService', { failureThreshold: 3, resetTimeoutMs: 6000 });
      this.sagaOrchestrator = new SagaOrchestrator({
        accountService: this.accountService,
        fraudService: this.fraudService,
        notificationService: this.notificationService
      }, this.eventBroker);

      this.apiGateway = new ApiGateway(this.eventBroker, { FraudDetectionService: this.fraudCircuitBreaker });
    }

    startUI() {
      this.dashboardManager = new DashboardManager(this);
      console.log('✅ NexusBank Core iniciado y funcional.');
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    window.nexusBankApp = new NexusBankApp();
    window.nexusBankApp.startUI();
  });

})();
