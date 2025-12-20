# 🌦️ DataOps Pipeline: Monitoramento Climático & Observabilidade

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black?logo=apachekafka&logoColor=white)
![Apache Druid](https://img.shields.io/badge/Apache_Druid-OLAP-cyan?logo=apache&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-blue?logo=docker&logoColor=white)

Este repositório contém o código-fonte e a infraestrutura de um projeto de **Iniciação Científica (IC)** desenvolvido na **UNIFEI** (Universidade Federal de Itajubá).

O projeto implementa um pipeline de Engenharia de Dados focado em **DataOps**, capaz de ingerir, processar e analisar dados meteorológicos em tempo real, além de possuir um sistema de **auto-telemetria** para monitorar a saúde da própria ingestão.

## 🏗️ Arquitetura do Projeto

O sistema opera em um fluxo contínuo de Streaming:

1.  **Fonte de Dados:** API NASA POWER (Dados horários de Temperatura, Umidade, Radiação Solar e Vento).
2.  **Ingestão (Producers):** Scripts Python otimizados com **Decorators** (`@monitor_performance`) para instrumentação automática.
3.  **Message Broker:** Apache Kafka (Gerenciamento de filas e desacoplamento).
4.  **Armazenamento OLAP:** Apache Druid (Análise exploratória e agregações em tempo real).

### 🔄 Fluxos de Dados (Topics)

- `clima-sudeste-raw`: Dados meteorológicos brutos de múltiplas cidades (SP, Itajubá, RJ, BH).
- `system-metrics`: Metadados de performance (Latência de API, Tempo de ingestão Kafka, Taxas de Erro).

## 🚀 Funcionalidades Chave

- **Ingestão Multi-Cidades:** Monitoramento simultâneo de diversas localidades geográficas.
- **Self-Monitoring (Observabilidade):** O sistema monitora a si mesmo. O código Python envia métricas de latência e throughput para o Druid, permitindo benchmarks de performance (Kafka vs API Externa).
- **Schema Evolution:** Suporte a dados JSON complexos e tipagem rigorosa no Druid.
- **Alta Disponibilidade:** Infraestrutura totalmente containerizada via Docker Compose.

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Python 3.12+ (Libs: `kafka-python`, `requests`).
- **Streaming:** Apache Kafka & Zookeeper.
- **Banco de Dados:** Apache Druid (Historical, MiddleManager, Broker, Coordinator).
- **Infraestrutura:** Docker & Docker Compose.

## 📦 Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados.
- Python 3.x instalado.

### 1. Subir a Infraestrutura

```bash
docker-compose up -d
```
