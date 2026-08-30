/**
 * Microservices.js - Microservicios Core del Banco NexusBank
 */

export class AccountService {
  constructor(eventBroker) {
    this.eventBroker = eventBroker;
    this.accounts = new Map([
      ['ACC-9988-COLL', { owner: 'Juan Pérez (Cuenta Nómina)', balance: 12850.00, currency: 'USD' }],
      ['ACC-5544-SAVE', { owner: 'María Rodríguez (Cuenta Ahorros)', balance: 5400.00, currency: 'USD' }],
      ['ACC-1122-DEST', { owner: 'Carlos Gómez (Cuenta Destino)', balance: 1200.00, currency: 'USD' }]
    ]);
  }

  async debitAccount(accountNum, amount) {
    const acc = this.accounts.get(accountNum);
    if (!acc) return { success: false, reason: 'Cuenta origen inexistente' };
    if (acc.balance < amount) return { success: false, reason: `Fondos insuficientes (Saldo actual: $${acc.balance})` };

    acc.balance -= amount;
    this.eventBroker.publish('account.debited', { accountNum, amount, newBalance: acc.balance }, 'AccountService');
    return { success: true, newBalance: acc.balance };
  }

  async creditAccount(accountNum, amount) {
    const acc = this.accounts.get(accountNum);
    if (!acc) return { success: false, reason: 'Cuenta destino inexistente' };

    acc.balance += amount;
    this.eventBroker.publish('account.credited', { accountNum, amount, newBalance: acc.balance }, 'AccountService');
    return { success: true, newBalance: acc.balance };
  }

  getAccountsState() {
    return Array.from(this.accounts.entries()).map(([num, data]) => ({ num, ...data }));
  }
}

export class FraudDetectionService {
  constructor(eventBroker) {
    this.eventBroker = eventBroker;
    this.shouldFailNetwork = false; // Inyección de fallo de red
    this.shouldTriggerHighRisk = false; // Inyección de sospecha de fraude
  }

  setNetworkFault(failState) {
    this.shouldFailNetwork = failState;
    console.log(`[FraudDetectionService] Fallo de Red inyectado: ${failState}`);
  }

  async evaluateRisk(accountNum, amount) {
    if (this.shouldFailNetwork) {
      throw new Error('503 Service Unavailable: Motor de Inteligencia Antifraude fuera de línea (Timeout de Red)');
    }

    if (amount > 10000 || this.shouldTriggerHighRisk) {
      return { approved: false, riskScore: 92, reason: 'Monto atípico excede umbral de seguridad para transferencia directa' };
    }

    const riskScore = Math.floor(5 + Math.random() * 15);
    return { approved: true, riskScore, reason: 'Riesgo Bajo' };
  }
}

export class NotificationService {
  constructor(eventBroker) {
    this.eventBroker = eventBroker;
    this.notificationsLog = [];

    this.eventBroker.subscribe('transfer.saga_completed', 'NotificationService', (evt) => {
      this.sendNotification(`📲 SMS/Push: Transferencia de $${evt.payload.amount} REALIZADA CON ÉXITO. Ref: ${evt.payload.transferId}`);
    });

    this.eventBroker.subscribe('transfer.saga_compensated', 'NotificationService', (evt) => {
      this.sendNotification(`🚨 Alerta Bancaria: Transferencia DENEGADA. Se realizó la devolución del dinero a su cuenta. Motivo: ${evt.payload.reason}`);
    });
  }

  sendNotification(message) {
    const notif = { id: `SMS-${Date.now()}`, message, timestamp: new Date().toLocaleTimeString() };
    this.notificationsLog.unshift(notif);
  }

  getNotifications() {
    return this.notificationsLog;
  }
}
