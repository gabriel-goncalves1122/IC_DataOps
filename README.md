# 🌦️🔥 DataOps Pipeline: Monitoramento Climático & Focos de Incêndio

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black?logo=apachekafka&logoColor=white)
![Apache Druid](https://img.shields.io/badge/Apache_Druid-OLAP-cyan?logo=apache&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache_Superset-Viz-red?logo=apache&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-blue?logo=docker&logoColor=white)

Este repositório contém o código-fonte e a infraestrutura de um projeto de **Iniciação Científica (IC)** desenvolvido na **UNIFEI** (Universidade Federal de Itajubá).

O projeto implementa uma arquitetura moderna de **DataOps** para ingestão e análise de Big Data em tempo real, orquestrada via Docker.

---

## 🏗️ Arquitetura do Projeto

O sistema opera com dois pipelines principais de ingestão:

1.  **Pipeline Climático:**
    - **Fonte:** API NASA POWER (Dados horários).
    - **Foco:** Temperatura, Umidade, Radiação Solar.
    - **Mecanismo:** Requests HTTP com instrumentação de performance.

2.  **Pipeline Geoespacial (Queimadas):**
    - **Fonte:** INPE BDQueimadas (CSVs massivos).
    - **Foco:** Focos de calor, FRP (Fire Radiative Power) e Risco de Fogo.
    - **Mecanismo:** Processamento em Batch/Streaming (Chunking) para arquivos grandes.

**Stack Tecnológica:**

- **Ingestão:** Scripts Python otimizados (`kafka-python`, `pandas`).
- **Buffer:** Apache Kafka (Desacoplamento).
- **Armazenamento:** Apache Druid (Banco OLAP com extensão espacial).
- **Visualização:** Apache Superset (Dashboards e Mapas).

---

## 🚀 Como Executar (Setup Inicial)

### 1. Subir a Infraestrutura

Este comando inicia o Kafka, Zookeeper, Cluster Druid e o Superset (via imagem customizada).

```bash
docker compose up -d
```
