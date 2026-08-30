/**
 * SagaOrchestrator.js - Patrón Saga para Transferencias Interbancarias
 * Orquesta transacciones financieras distribuidas y ejecuta reversiones compensatorias
 * en caso de rechazo por fraude o indisponibilidad del banco receptor.
 */

export class SagaOrchestrator {
  constructor(services, eventBroker) {
    this.services = services; // { accountService, fraudService, notificationService }
    this.eventBroker = eventBroker;
    this.sagaLogs = [];
  }

  async executeInterbankTransferSaga(transferData) {
    const sagaId = `SAGA-BANK-${Date.now()}`;
    const log = {
      sagaId,
      transferId: transferData.transferId,
      stepsCompleted: [],
      status: 'IN_PROGRESS',
      startTime: new Date().toISOString(),
      details: []
    };
    this.sagaLogs.unshift(log);

    console.log(`[SagaOrchestrator:${sagaId}] Iniciando Saga de Transferencia Bancaria por $${transferData.amount}`);
    this.recordLog(log, `Paso 1: Iniciando validación para cuenta origen ${transferData.sourceAccount}`);

    try {
      // Paso 1: Debitar Saldo de la Cuenta Origen
      this.recordLog(log, `Paso 1: Debitando $${transferData.amount} de la cuenta ${transferData.sourceAccount}...`);
      const debitRes = await this.services.accountService.debitAccount(transferData.sourceAccount, transferData.amount);
      if (!debitRes.success) throw new Error(`Saldo insuficiente o cuenta bloqueada: ${debitRes.reason}`);
      log.stepsCompleted.push({ step: 'DEBIT_SENDER', rollbackData: { account: transferData.sourceAccount, amount: transferData.amount } });
      this.recordLog(log, `✓ Débito exitoso. Nuevo saldo disponible: $${debitRes.newBalance}`);

      // Paso 2: Evaluación Antifraude en Tiempo Real (Protegido por Circuit Breaker)
      this.recordLog(log, `Paso 2: Evaluando riesgo de fraude en FraudDetectionService...`);
      const fraudRes = await this.services.fraudService.evaluateRisk(transferData.sourceAccount, transferData.amount);
      if (!fraudRes.approved) throw new Error(`Alerta de Fraude: Transacción bloqueada por política de seguridad (${fraudRes.reason})`);
      log.stepsCompleted.push({ step: 'FRAUD_CHECK', rollbackData: null });
      this.recordLog(log, `✓ Antifraude APROBADO (Risk Score: ${fraudRes.riskScore}/100)`);

      // Paso 3: Acreditar Cuenta Destino
      this.recordLog(log, `Paso 3: Acreditando $${transferData.amount} a la cuenta destino ${transferData.targetAccount}...`);
      const creditRes = await this.services.accountService.creditAccount(transferData.targetAccount, transferData.amount);
      if (!creditRes.success) throw new Error(`Banco destino no responde o cuenta inválida: ${creditRes.reason}`);
      log.stepsCompleted.push({ step: 'CREDIT_RECEIVER', rollbackData: { account: transferData.targetAccount, amount: transferData.amount } });
      this.recordLog(log, `✓ Crédito interbancario exitoso en cuenta ${transferData.targetAccount}`);

      // Saga Finalizada con Éxito
      log.status = 'COMPLETED';
      log.endTime = new Date().toISOString();
      this.recordLog(log, `🎉 TRANSFERENCIA EXITOSA. Comprobante bancario registrado.`);

      this.eventBroker.publish('transfer.saga_completed', { sagaId, transferId: transferData.transferId, amount: transferData.amount }, 'SagaOrchestrator');
      return { success: true, sagaId, log };

    } catch (error) {
      console.error(`[SagaOrchestrator:${sagaId}] RECHAZO EN SAGA BANCARIA: ${error.message}`);
      this.recordLog(log, `❌ FALLO EN TRANSFERENCIA: ${error.message}. Iniciando Transacciones Compensatorias...`);
      
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
          this.recordLog(log, ` [Compensación] REEMBOLSANDO $${stepInfo.rollbackData.amount} a la cuenta origen ${stepInfo.rollbackData.account}...`);
          await this.services.accountService.creditAccount(stepInfo.rollbackData.account, stepInfo.rollbackData.amount);
          break;
      }
    }
    this.recordLog(log, `✓ Reversión compensatoria completada. Fondos restituidos al cliente de origen.`);
  }

  recordLog(logObj, message) {
    const entry = `[${new Date().toLocaleTimeString()}] ${message}`;
    logObj.details.push(entry);
  }

  getLogs() {
    return this.sagaLogs;
  }
}
