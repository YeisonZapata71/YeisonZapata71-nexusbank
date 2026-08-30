# 🏛️ NexusBank Core Cloud Architecture
> **Enterprise Banking Core & Distributed Microservices Platform**  
> *Cloud-Native Event-Driven Architecture (EDA) for High-Availability Financial Systems*

[![License: MIT](https://img.shields.gl/badge/License-MIT-blue.svg)](LICENSE)
[![Architecture: Cloud-Native](https://img.shields.gl/badge/Architecture-Cloud--Native-1a73e8.svg?logo=google-cloud)](https://cloud.google.com)
[![Compliance: PCI-DSS](https://img.shields.gl/badge/Compliance-PCI--DSS-34a853.svg)](src/gateway/ApiGateway.js)
[![Pattern: Saga Orchestration](https://img.shields.gl/badge/Pattern-Saga%20Orchestrator-ea4335.svg)](src/patterns/SagaOrchestrator.js)
[![Pattern: CQRS](https://img.shields.gl/badge/Pattern-CQRS%20Redis-fbbc04.svg)](src/patterns/CQRS.js)

---

## 📌 System Overview

**NexusBank Core** is a next-generation distributed cloud banking platform designed to process high-throughput interbank financial transactions in **Pesos Colombianos ($ COP)** while ensuring strict ACID compliance, data consistency, and **99.999% High Availability (HA)**.

Faced with traditional monolithic banking bottlenecks (lock contention on core ledger databases during bi-weekly payroll spikes and single points of failure), NexusBank Core implements an **Event-Driven Microservices Architecture** backed by industry-standard cloud resilience patterns.

---

## 📐 Architecture Topology

```mermaid
graph TB
    subgraph ClientLayer["🌐 Client Access Layer"]
        MobileApp["NexusBank Mobile App (iOS / Android)"]
        WebBanking["Web Banking SPA (React)"]
    end

    subgraph EdgeLayer["🛡️ Edge & Security Layer"]
        WAF["AWS WAF (DDoS Shield)"]
        Gateway["Kong API Gateway (Rate Limiting & Auth)"]
    end

    subgraph MicroservicesCluster["☸️ Microservices Cluster (AWS EKS Multi-AZ)"]
        SagaPod["Saga Orchestrator Service"]
        AccountPod["Account & Ledger Service"]
        FraudPod["Anti-Fraud Engine (Circuit Breaker)"]
        NotifPod["Notification Engine"]
    end

    subgraph EventBus["📡 Distributed Messaging Bus"]
        Broker["Event Broker Pub/Sub (Apache Kafka)"]
    end

    subgraph Persistence["🗄️ CQRS Persistence Tier"]
        AuroraWrite[("AWS Aurora PostgreSQL (Write Ledger DB)")]
        RedisRead[("ElastiCache Redis (CQRS Read View Cache)")]
        HSM Vault[("Hardware Security Module (HSM Keys)")]
    end

    MobileApp --> WAF
    WebBanking --> WAF
    WAF --> Gateway
    Gateway --> SagaPod
    SagaPod --> AccountPod
    SagaPod --> FraudPod
    SagaPod --> Broker
    AccountPod --> AuroraWrite
    Broker --> RedisRead
    Gateway --> HSM Vault
```

---

## ⚙️ Key Architectural Patterns

### 1. API Gateway Pattern (Rate Limiting & Security)
- **Token Bucket Algorithm:** Enforces a strict rate limit (10 requests/sec capacity) to mitigate brute-force and DDoS attacks on banking endpoints.
- **Centralized Security:** Handles JWT signature validation and mTLS 1.3 encryption headers before request routing.

### 2. Circuit Breaker Pattern (Fault Isolation)
- **State Machine:** Manages state transitions across `CLOSED` ➔ `OPEN` ➔ `HALF-OPEN`.
- **Fast-Fail Protection:** Automatically trips to `OPEN` when the `FraudDetectionService` encounters 3 consecutive timeouts (HTTP 503), short-circuiting downstream calls and invoking fallback risk assessments to prevent thread starvation.

### 3. CQRS (Command Query Responsibility Segregation)
- **Write Path (Command Store):** Executes ACID-compliant ledger transactions written to `Aurora PostgreSQL`.
- **Read Path (Query Store):** Projections are asynchronously synchronized via Kafka events into materialized `Redis` views, delivering sub-30ms mobile balance checks.

### 4. Saga Orchestration Pattern (Distributed Transactions)
- **LIFO Compensation:** Replaces heavy 2-Phase Commit (2PC) database locks. Coordinates multi-step transfers:
  1. `debitAccount(SourceAcc, Amount)`
  2. `evaluateRisk(SourceAcc, Amount)`
  3. `creditAccount(TargetAcc, Amount)`
- If step 2 or 3 fails or times out, the orchestrator triggers backward compensating transactions (`creditAccount(SourceAcc)`) to refund the customer's funds immediately.

---

## 📂 Repository Structure

```text
ARQUITECTURA_DE_SOFTWARE/
├── index.html                   # Banking Dashboard & Live Simulator UI
├── styles.css                   # Google Material Design System
├── app.js                       # Standalone Application Orchestrator & Bootstrapper
├── package.json                 # Project configuration & start scripts
├── README.md                    # Repository Technical Documentation
└── src/                         # Core Source Code
    ├── broker/
    │   └── EventBroker.js       # Financial Pub/Sub Event Broker
    ├── gateway/
    │   └── ApiGateway.js        # Security Gateway with Token Bucket Rate Limiter
    ├── patterns/
    │   ├── CircuitBreaker.js    # Circuit Breaker Resilience Module
    │   ├── CQRS.js              # Command & Query Stores (Read View Projections)
    │   └── SagaOrchestrator.js  # Interbank Transfer Saga Orchestrator
    ├── services/
    │   └── Microservices.js     # Account, Anti-Fraud & Notification Microservices
    └── ui/
        └── DashboardManager.js  # Live UI Telemetry & Event Stream Manager
```

---

## 📊 Quality Attribute Benchmarks (SLOs / SLAs)

| Quality Attribute | Target SLO / SLA | Architectural Solution |
| :--- | :--- | :--- |
| **High Availability (HA)** | **99.999% Uptime** (Max 5.26 min unplanned downtime/year) | Multi-AZ Kubernetes cluster with Circuit Breaker fault isolation. |
| **Read Latency** | **< 30 ms** for 99% of mobile account queries | CQRS separation with Redis in-memory materialized statement cache. |
| **Throughput** | **15,000 TPS** capacity at API Gateway | Token Bucket rate limiting with asynchronous Kafka Pub/Sub event bus. |
| **Data Consistency** | **Eventual Consistency** with zero lock contention | Saga Orchestrator with automated LIFO compensating refund transactions. |
| **Compliance** | **PCI-DSS Level 1 & GDPR** | End-to-end mTLS 1.3 encryption and Hardware Security Module (HSM) key vault. |

---

## ⚡ Quick Start

### Prerequisites
- Node.js (v16+) or any standard static HTTP web server.

### Launching the Interactive Simulator
1. Open the project root directory in your terminal.
2. Start the local server:
   ```bash
   python -m http.server 3000
   ```
3. Access the Banking Dashboard at `http://localhost:3000/index.html`. Alternatively, double-click `index.html` directly to open in any web browser.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
