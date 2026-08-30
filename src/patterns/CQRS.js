/**
 * CQRS.js - Patrón CQRS Bancario (Command Query Responsibility Segregation)
 * Separa el Libro Mayor Transaccional ACID (Escrituras) de las Vistas Materializadas en Caché para App Móvil (Lecturas)
 */

export class WriteCommandStore {
  constructor(eventBroker) {
    this.eventBroker = eventBroker;
    // Libro Mayor de Contabilidad Bancaria (PostgreSQL ACID Ledger)
    this.writeLedgerDb = new Map();
  }

  executeCommand(commandName, payload) {
    const commandId = `CMD-BANK-${Date.now()}`;
    console.log(`[CQRS:LedgerWrite] Comando Bancario: ${commandName}`, payload);

    switch (commandName) {
      case 'ExecuteTransfer':
        const txRecord = {
          id: payload.transferId || `TX-NEXUS-${Math.floor(100000 + Math.random() * 900000)}`,
          sourceAccount: payload.sourceAccount,
          targetAccount: payload.targetAccount,
          amount: payload.amount,
          status: 'COMPLETED',
          timestamp: new Date().toISOString()
        };
        this.writeLedgerDb.set(txRecord.id, txRecord);
        this.eventBroker.publish('transfer.executed', txRecord, 'CQRS-WriteLedger');
        return { success: true, commandId, data: txRecord };

      case 'RollbackTransfer':
        if (this.writeLedgerDb.has(payload.transferId)) {
          const tx = this.writeLedgerDb.get(payload.transferId);
          tx.status = 'REVERSED_REFUNDED';
          this.writeLedgerDb.set(tx.id, tx);
          this.eventBroker.publish('transfer.reversed', tx, 'CQRS-WriteLedger');
          return { success: true, commandId, data: tx };
        }
        return { success: false, error: 'Transacción no encontrada en el Libro Mayor' };

      default:
        throw new Error(`Comando bancario ${commandName} desconocido.`);
    }
  }
}

export class ReadQueryStore {
  constructor(eventBroker) {
    this.eventBroker = eventBroker;
    // Vista Materializada de Consultas Móviles (Redis Cache - Resumen de Saldos)
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
      formattedAmount: `$${tx.amount.toLocaleString('es-CO', { minimumFractionDigits: 2 })}`,
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
