# Roadmap 2024

## Key Goals

1. **K8s Operator for Multi-Transfer Deployments**
2. **Delta Sink**
3. **Iceberg Sink**
3. **Clickhouse Exactly Once Support**

---

## 1. E2E Testing for Main Connectors

### Objective:
Set up comprehensive **end-to-end tests** in the CI pipeline for the following main connectors:
- **Postgres**
- **MySQL**
- **Clickhouse**
- **Yandex Database (YDB)**
- **YTsaurus (YT)**

### Steps:
- [x] Configure test environments in CI for each connector.
- [x] Design E2E test scenarios covering various transfer modes (snapshot, replication, etc.).
- [x] Automate test execution for all supported connectors.
- [x] Set up reporting and logs for test failures.

### Milestone:
Achieve **fully automated E2E testing** across all major connectors to ensure continuous integration stability.

---

## 2. Helm Deployment Documentation

### Objective:
Provide detailed documentation on deploying the transfer engine using **Helm** on Kubernetes clusters.

### Steps:
- [x] Create Helm chart for easy deployment of the transfer engine.
- [x] Write comprehensive **Helm deployment guide**.
  - [x] Define key parameters for customization (replicas, resources, etc.).
  - [x] Instructions for various environments (local, cloud).
- [x] Test Helm deployment process on common platforms (GKE, EKS, etc.).

### Milestone:
Enable seamless deployment of the transfer engine via Helm with clear and accessible documentation.

---

## Summary

- **Q2-Q3**: Focus on **E2E testing** for core connectors.
- **Q3**: Publish **Helm deployment** documentation and final testing.
- **Q3-Q4**: Develop and release the **Kubernetes operator** for multi-transfer management.

This roadmap aims to enhance testing, simplify deployment, and provide advanced scalability options for the transfer engine.
