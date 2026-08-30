/**
 * DashboardManager.js - Gestor de Interfaz de Usuario NexusBank Core
 */

export class DashboardManager {
  constructor(appContext) {
    this.app = appContext;
    this.initEventListeners();
    this.initGlobalEventSubscribers();
    this.renderAll();

    setInterval(() => this.updateMetricsUI(), 1000);
  }

  initEventListeners() {
    // Transferencia bancaria normal
    document.getElementById('btn-execute-transfer')?.addEventListener('click', () => {
      this.handleExecuteTransfer();
    });

    // Inyección de fallo en Motor Antifraude
    document.getElementById('btn-inject-fraud-fault')?.addEventListener('click', () => {
      const btn = document.getElementById('btn-inject-fraud-fault');
      const isActive = btn.classList.toggle('active-fault');

      this.app.fraudService.setNetworkFault(isActive);
      btn.innerText = isActive ? '🔥 Caída Motor Antifraude: ACTIVADO (503 Error)' : '⚡ Inyectar Caída en Motor Antifraude';
      btn.style.background = isActive ? '#d93025' : '#ea4335';
      btn.style.color = '#ffffff';
    });

    // Simulación de ráfaga Rate Limit
    document.getElementById('btn-rate-limit-spike')?.addEventListener('click', () => {
      this.triggerRateLimitSpike();
    });

    // Reset Circuit Breaker
    document.getElementById('btn-reset-cb')?.addEventListener('click', () => {
      this.app.fraudCircuitBreaker.forceReset();
      this.renderCircuitBreakerUI();
    });

    // Pestañas UML
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
    const amount = parseFloat(document.getElementById('input-amount').value) || 100;

    if (sourceAccount === targetAccount) {
      alert('La cuenta de origen y destino deben ser diferentes.');
      return;
    }

    const transferData = {
      transferId: `TRF-${Math.floor(100000 + Math.random() * 900000)}`,
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
            <th>Monto</th>
            <th>Estado (CQRS Cache)</th>
            <th>Hora</th>
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
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
        <td><strong style="color:#1a73e8;">$${a.balance.toLocaleString('es-CO', { minimumFractionDigits: 2 })} ${a.currency}</strong></td>
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
