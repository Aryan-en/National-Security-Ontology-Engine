# National Security Ontology Engine (NSOE)

> **Production-Grade Threat Intelligence Platform**
> Integrating CCTV surveillance, cybersecurity telemetry, and social media intelligence
> through a unified ontology/knowledge graph for real-time national security threat detection.

---

## Table of Contents

1. [Mission and Scope](#1-mission-and-scope)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [Ontology and Knowledge Graph Design](#3-ontology-and-knowledge-graph-design)
4. [End-to-End Data Flow](#4-end-to-end-data-flow)
5. [AI/ML Models](#5-aiml-models)
6. [Tech Stack](#6-tech-stack)
7. [Scalability Design — 10,000+ Edge Nodes](#7-scalability-design--10000-edge-nodes)
8. [Failure Handling and Fault Tolerance](#8-failure-handling-and-fault-tolerance)
9. [Security Architecture](#9-security-architecture)
10. [Real-Time vs Batch Processing](#10-real-time-vs-batch-processing)
11. [Deployment Architecture — Edge + Cloud Hybrid](#11-deployment-architecture--edge--cloud-hybrid)
12. [India-Specific Deployment Constraints](#12-india-specific-deployment-constraints)
13. [Trade-offs and Limitations](#13-trade-offs-and-limitations)
14. [Cost Considerations](#14-cost-considerations)
15. [Ethical Concerns and Governance](#15-ethical-concerns-and-governance)
16. [Status and Roadmap](#16-status-and-roadmap)

---

## 1. Mission and Scope

The National Security Ontology Engine (NSOE) is a production-grade, semantically rich intelligence fusion platform. It aggregates signals from three primary source domains:

- **Physical surveillance**: CCTV cameras at borders, transport hubs, public spaces, critical infrastructure
- **Cyber domain**: Firewall logs, IDS/IPS alerts, threat feeds, dark web monitoring, OSINT
- **Social media intelligence (SOCMINT)**: Open platforms, encrypted channel monitoring (where lawfully authorized), news aggregation

The platform connects these sources through a **common ontology** and **knowledge graph**, enabling analysts and automated systems to detect, reason about, and respond to threats that span multiple domains — e.g., a person flagged on a watchlist (physical), communicating on an encrypted channel (SOCMINT), and accessing a critical infrastructure network (cyber).

---

## 2. System Architecture Overview

The architecture is divided into five tiers:

```
┌─────────────────────────────────────────────────────────────────┐
│  TIER 1 — SOURCE LAYER                                          │
│  CCTV Cameras · Network Sensors · Social Media APIs · OSINT     │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│  TIER 2 — EDGE LAYER                                            │
│  Edge Inference Nodes · Local Buffering · Protocol Adapters     │
│  Hardware: Jetson Orin / Raspberry Pi CM4 / x86 Mini-PC         │
└────────────────────────┬────────────────────────────────────────┘
                         │  (MQTT / gRPC / HTTPS / RTSP proxy)
┌────────────────────────▼────────────────────────────────────────┐
│  TIER 3 — INGESTION AND STREAM PROCESSING LAYER                 │
│  Apache Kafka · Apache Flink · Schema Registry · CDC pipelines  │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│  TIER 4 — INTELLIGENCE CORE (ONTOLOGY ENGINE)                   │
│  Knowledge Graph · Ontology Server · Correlation Engine         │
│  Entity Resolution · Rule Engine · ML Inference Service         │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│  TIER 5 — ANALYST AND RESPONSE LAYER                            │
│  Dashboard · Alert Management · Case Management · APIs          │
│  Automated Playbooks · Audit and Compliance                     │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

- **Federated intelligence**: Each tier can operate semi-independently, preventing single points of failure.
- **Ontology-first**: All data is modeled through a canonical ontology before entering the graph — no raw data in the knowledge layer.
- **Event-driven**: All inter-tier communication uses event streaming (Kafka) rather than synchronous API calls to prevent cascading failures.
- **Zero-trust security**: Every service authenticates and authorizes every request regardless of network location.
- **Human-in-the-loop**: Automated systems raise alerts; humans make consequential decisions.

---

## 3. Ontology and Knowledge Graph Design

### 3.1 Ontology Model

The NSOE ontology is built in OWL 2 (Web Ontology Language) and serialized in Turtle/JSON-LD. It defines the following top-level class hierarchy:

```
owl:Thing
├── nsoe:Agent
│   ├── nsoe:Person
│   │   ├── nsoe:SuspectIndividual
│   │   └── nsoe:WatchlistEntity
│   ├── nsoe:Organization
│   │   ├── nsoe:TerroristGroup
│   │   ├── nsoe:CriminalNetwork
│   │   └── nsoe:StateActor
│   └── nsoe:AutonomousSystem (bots, malware agents)
│
├── nsoe:Event
│   ├── nsoe:PhysicalEvent
│   │   ├── nsoe:Sighting (CCTV detection event)
│   │   ├── nsoe:BorderCrossing
│   │   └── nsoe:SecurityIncident
│   ├── nsoe:CyberEvent
│   │   ├── nsoe:Intrusion
│   │   ├── nsoe:DataExfiltration
│   │   └── nsoe:MalwareExecution
│   └── nsoe:SocialMediaEvent
│       ├── nsoe:ThreatPost
│       ├── nsoe:RadicalizationSignal
│       └── nsoe:CoordinationEvent
│
├── nsoe:Location
│   ├── nsoe:GeographicArea
│   ├── nsoe:CriticalInfrastructure
│   └── nsoe:DigitalAsset (IP range, domain, ASN)
│
├── nsoe:Artifact
│   ├── nsoe:Weapon
│   ├── nsoe:Document
│   ├── nsoe:Malware
│   └── nsoe:CryptoWallet
│
└── nsoe:ThreatCampaign
    ├── nsoe:TerrorCampaign
    └── nsoe:CyberCampaign
```

**Key Object Properties (Relationships)**:

| Property | Domain | Range | Meaning |
|---|---|---|---|
| `nsoe:sightedAt` | Person | Location | Person detected at a location |
| `nsoe:participatedIn` | Agent | Event | Actor involved in event |
| `nsoe:communicatedWith` | Agent | Agent | Detected communication |
| `nsoe:operatesInfrastructure` | Agent | DigitalAsset | Controls network resource |
| `nsoe:associatedCampaign` | Event | ThreatCampaign | Event tied to a campaign |
| `nsoe:hasRiskScore` | Agent/Event | xsd:float | Risk score [0–1] |
| `nsoe:corroboratedBy` | any | any | Evidence provenance link |
| `nsoe:temporallyRelatedTo` | Event | Event | Temporal causal chain |

### 3.2 Knowledge Graph Construction

**Graph Database**: Neo4j Enterprise (primary) with Amazon Neptune as a disaster-recovery replica.

**Graph Schema Enforcement**: SHACL (Shapes Constraint Language) constraints validate all nodes/edges before insertion.

**Provenance**: Every node and edge carries:
- `source_id` — which pipeline/feed produced it
- `ingested_at` — UTC timestamp of ingestion
- `confidence_score` — float [0–1] from producing model
- `classification_level` — RESTRICTED / SECRET / TOP-SECRET

**Temporal Graph**: The graph maintains a bitemporal model:
- **Valid time** — when the fact was true in the real world
- **Transaction time** — when the system recorded it
This enables timeline reconstruction for investigations.

### 3.3 Threat Detection via Graph Reasoning

The ontology engine applies three layers of reasoning:

**Layer 1 — Rule-Based Inference (SWRL + Drools)**
```
# Example rule: co-location of two watchlist persons triggers alert
IF Person A is sightedAt Location L at time T1
AND Person B is sightedAt Location L at time T2
AND abs(T1 - T2) < 300 seconds
AND A rdf:type WatchlistEntity
AND B rdf:type WatchlistEntity
THEN ASSERT CoordinationEvent(A, B, L) with confidence 0.85
```

**Layer 2 — Graph Pattern Matching (Cypher / SPARQL)**
```cypher
// Detect terrorist cell structure: hub + 3+ associates
MATCH (hub:Person)-[:COMMUNICATES_WITH]->(member:Person)
WHERE hub.watchlist_flag = true
WITH hub, count(member) AS associates
WHERE associates >= 3
MATCH path = (hub)-[:COMMUNICATES_WITH*1..3]->(leaf:Person)
RETURN path, hub.risk_score ORDER BY hub.risk_score DESC
```

**Layer 3 — Graph Neural Network (PyTorch Geometric)**
GNN-based link prediction identifies probable hidden connections between entities not yet explicitly observed. Used for:
- Predicting undiscovered members of a known network
- Forecasting likely next targets or locations
- Surfacing dormant sleeper cell patterns

---

## 4. End-to-End Data Flow

### 4.1 CCTV Stream → Edge → Central Intelligence

```
[Camera]
  │ RTSP / ONVIF stream (H.264/H.265)
  ▼
[Edge Node — Jetson Orin NX]
  ├── Stream ingestion via GStreamer pipeline
  ├── Frame sampling (configurable: 1–10 FPS)
  ├── YOLOv8 + DeepSORT: object detection + person tracking
  ├── ArcFace face recognition (if enabled by policy)
  ├── Anomaly scoring (crowd density, loitering, abandoned object)
  ├── Metadata extraction: bounding boxes, track IDs, timestamps, confidence
  ├── Local SQLite buffer (up to 72h of metadata in degraded connectivity)
  └── Publish to central Kafka topic: `edge.cctv.events`
        via MQTT-over-TLS or gRPC (depending on bandwidth)

[Kafka: edge.cctv.events]
  ▼
[Apache Flink — Stream Processor]
  ├── Deduplication (sliding window, 30s)
  ├── Clock normalization (NTP-corrected timestamps)
  ├── Enrichment join: geolocation lookup, camera metadata
  └── Emit to: `enriched.cctv.events`

[Ontology Ingestion Service]
  ├── Maps detection to nsoe:Sighting entity
  ├── Links to nsoe:Location (camera GPS coordinates)
  ├── Resolves face match against Person entities in graph
  └── Writes to Neo4j with provenance metadata

[Correlation Engine]
  ├── Pattern matching against known threat rules
  ├── Cross-domain join: does this face match a SOCMINT signal?
  └── Emits: nsoe:ThreatAlert or nsoe:CoordinationEvent

[Alert Management Service]
  └── Push notification to analyst dashboard + case management
```

### 4.2 Cyber Telemetry Flow

```
[Network sensors, EDR agents, firewall logs, threat feeds]
  ▼
[Kafka: cyber.raw.events]
  ▼
[Flink: normalize to STIX 2.1 objects]
  ▼
[Ontology Ingestion: create CyberEvent, TTP, Indicator nodes]
  ▼
[Correlation Engine: MITRE ATT&CK mapping, lateral movement detection]
  ▼
[Graph: link Indicators → ThreatActor → Campaign]
```

### 4.3 Social Media Intelligence Flow

```
[Twitter/X API · Telegram scraper · News RSS · Dark web crawlers]
  ▼
[NLP Pipeline: multilingual NER, intent classification, sentiment]
  ▼
[Kafka: socmint.processed.events]
  ▼
[Ontology Ingestion: create SocialMediaEvent, extract entities]
  ▼
[Entity Resolution: match social handles to Person/Organization]
  ▼
[Graph: link post → author → network → campaign]
  ▼
[Credibility scoring: cross-validate with other domain signals]
```

---

## 5. AI/ML Models

### 5.1 Computer Vision (Edge-deployed)

| Model | Purpose | Framework | Edge Hardware |
|---|---|---|---|
| YOLOv8n/s | Real-time object/person detection | TensorRT (quantized INT8) | Jetson Orin |
| DeepSORT / ByteTrack | Multi-object tracking across frames | ONNX Runtime | Jetson Orin |
| ArcFace (MobileNetV3 backbone) | Face recognition against watchlist | TensorRT | Jetson Orin |
| MobileNet-based crowd density | Crowd density estimation | TFLite | Raspberry Pi CM4 |
| Abandoned object detector | Static object anomaly detection | Custom YOLOv8 head | Jetson |

All edge models are quantized (INT8/FP16) and benchmarked for <30ms inference per frame at 4K resolution.

### 5.2 Natural Language Processing (Cloud-deployed)

| Model | Purpose | Framework |
|---|---|---|
| XLM-RoBERTa (multilingual) | Named Entity Recognition for 12 Indian languages + English/Urdu/Arabic | HuggingFace Transformers |
| IndicBERT fine-tuned | Hindi/regional language threat intent classification | PyTorch |
| mBART-50 | Cross-language entity linking and translation | HuggingFace |
| Custom BiLSTM-CRF | Structured information extraction from OSINT docs | PyTorch |
| Sentence-BERT | Semantic similarity for deduplication | ONNX Runtime |

### 5.3 Anomaly Detection

| Model | Purpose | Deployment |
|---|---|---|
| Isolation Forest | Network traffic anomaly detection | Flink ML (streaming) |
| LSTM Autoencoder | Temporal behavioral anomaly (movement patterns) | TF Serving |
| One-Class SVM | Novel attack vector detection in cyber domain | Scikit-learn serving |
| DBSCAN / HDBSCAN | Unusual clustering of social media accounts | Batch (Spark) |

### 5.4 Graph Learning

| Model | Purpose | Framework |
|---|---|---|
| GraphSAGE | Node classification: risk scoring of entities | PyTorch Geometric |
| GAT (Graph Attention Network) | Link prediction: probable hidden connections | PyTorch Geometric |
| RGCN (Relational GCN) | Multi-relational graph reasoning | DGL |
| Node2Vec embeddings | Entity similarity and clustering | NetworkX + Gensim |

### 5.5 Model Lifecycle Management

- **MLflow** for experiment tracking, model registry, and versioning.
- **Seldon Core** or **BentoML** for model serving with canary deployments.
- **Evidently AI** for automated data drift and concept drift detection.
- **Federated retraining**: Edge models retrained using aggregated statistics (no raw data uplink) via Flower federated learning framework.

---

## 6. Tech Stack

### 6.1 Backend Services

| Layer | Technology | Reason |
|---|---|---|
| API Gateway | Kong + gRPC gateway | Protocol translation, rate limiting |
| Microservices | Python (FastAPI) + Go | Python for ML-heavy services, Go for high-throughput pipelines |
| Service mesh | Istio | mTLS, traffic policies, observability |
| Workflow orchestration | Apache Airflow | Batch job scheduling, ontology refresh pipelines |
| Message broker | Apache Kafka (Confluent) | High-throughput event streaming, replay capability |
| Stream processing | Apache Flink | Stateful streaming, complex event processing |
| Batch processing | Apache Spark | Historical analysis, model retraining jobs |

### 6.2 Data Storage

| Store | Technology | Purpose |
|---|---|---|
| Knowledge Graph | Neo4j Enterprise 5.x | Primary ontology store, Cypher queries |
| Graph replica / SPARQL | Amazon Neptune | Federated SPARQL, DR replica |
| Time-series | Apache Cassandra + TimescaleDB | Camera events, telemetry streams |
| Object store | MinIO (on-prem) / S3 | Raw video clips, large blobs |
| Search | Elasticsearch 8.x | Full-text OSINT search, entity search |
| Cache | Redis Cluster | Session state, hot entity cache |
| Edge local buffer | SQLite + RocksDB | Offline event buffering on edge devices |
| Analytical warehouse | Apache Iceberg on S3 | Historical analysis, compliance archival |

### 6.3 ML Infrastructure

| Component | Technology |
|---|---|
| Model registry | MLflow + MinIO artifacts |
| Model serving | Triton Inference Server (NVIDIA) + BentoML |
| Experiment tracking | MLflow |
| Feature store | Feast |
| Drift monitoring | Evidently AI |
| Federated learning | Flower (Flwr) |
| Training compute | Kubernetes + GPU nodes (A100/H100) |

### 6.4 Frontend and Analyst Tools

| Tool | Technology |
|---|---|
| Analyst dashboard | React + TypeScript + Kepler.gl (geospatial) |
| Graph explorer | Neo4j Bloom + custom React graph UI (Sigma.js) |
| Alert management | Custom React + WebSocket push |
| Case management | Integration with OpenCTI or custom module |
| Mobile alerts | React Native |

### 6.5 Infrastructure and Platform

| Component | Technology |
|---|---|
| Container orchestration | Kubernetes (K8s) — on-prem + cloud hybrid |
| Container runtime | containerd |
| GitOps | ArgoCD |
| Secret management | HashiCorp Vault |
| Service identity | SPIFFE/SPIRE |
| Observability | Prometheus + Grafana + Loki + Jaeger |
| CI/CD | GitHub Actions + ArgoCD |
| IaC | Terraform + Ansible |
| Edge device management | K3s (lightweight Kubernetes for edge) + Akri |

---

## 7. Scalability Design — 10,000+ Edge Nodes

### 7.1 Edge Node Architecture

Each edge node runs a K3s agent with the following containerized workloads:
- **Camera adapter service**: Protocol-agnostic RTSP/ONVIF/proprietary stream ingestion
- **Inference runtime**: Triton Inference Server (lightweight profile)
- **Event publisher**: Kafka producer with local RocksDB buffer
- **Telemetry agent**: Prometheus node exporter + custom metrics

### 7.2 Hierarchical Aggregation

To avoid overloading central systems, a three-tier topology is used:

```
[10,000 Camera Edge Nodes] (Tier 1)
        │  ~50 nodes per cluster
[200 Regional Aggregation Nodes] (Tier 2)
        │  ~10 regional clusters per zone
[20 Zone Intelligence Hubs] (Tier 3)
        │
[Central Intelligence Core] (Cloud/DC)
```

- **Tier 1 → Tier 2**: Compressed, pre-filtered metadata only (not raw video)
- **Tier 2**: Runs regional Kafka brokers, performs local deduplication and correlation
- **Tier 3**: Runs regional graph partitions for local real-time queries
- **Central Core**: Receives only high-confidence, deduplicated alerts and aggregated statistics

### 7.3 Kafka Cluster Sizing

For 10,000 nodes producing 100 events/second each:
- **Throughput**: ~1M events/second
- **Kafka partitions**: 3,000 partitions across 30 brokers (100 partitions/broker)
- **Retention**: 24h hot (Kafka) → 30d warm (Iceberg) → 7y cold (S3 Glacier)
- **Consumer lag monitoring**: Alert if any consumer group lags > 10,000 messages

### 7.4 Graph Database Horizontal Scaling

- **Neo4j Fabric**: Horizontal sharding across multiple Neo4j instances by entity type
- **Read replicas**: 5 read replicas per region for dashboard query load
- **Cache layer**: Redis caches top 10,000 highest-risk entity subgraphs with 5-minute TTL
- **Query timeout enforcement**: All Cypher queries have 5s timeout; complex analytics run asynchronously

---

## 8. Failure Handling and Fault Tolerance

### 8.1 Edge Node Failure

- **Detection**: K3s health checks + custom heartbeat every 30s
- **Local buffering**: RocksDB stores up to 72h of events if network is unavailable
- **Recovery**: On reconnection, replay buffer to Kafka with original timestamps (not re-timestamped)
- **Graceful degradation**: If inference model fails to load, node enters "pass-through" mode — streams raw event metadata without ML enrichment

### 8.2 Kafka Broker Failure

- **Replication factor**: 3 (minimum 2 in-sync replicas required for producer ACK)
- **Controller failover**: KRaft (no ZooKeeper) — sub-second leader election
- **Cross-region replication**: MirrorMaker 2 replicates critical topics to secondary region

### 8.3 Graph Database Failure

- **Neo4j Causal Cluster**: 3 core members + N read replicas
- **Writes**: Only to leader; automatic re-election on leader failure (<15s)
- **Neptune replica**: Read-only failover for SPARQL queries during Neo4j maintenance
- **Write journal**: All graph writes also published to Kafka topic for replay on corruption

### 8.4 ML Model Failure

- **Circuit breaker**: If model inference >p99 latency threshold, bypass ML and use rule-based fallback
- **Model shadow deployment**: New models run in parallel with production model; only promoted after 24h of metric parity
- **Fallback scoring**: Pre-computed static risk scores from last batch run used if live inference fails

### 8.5 Clock Synchronization Failures

- **GPS-disciplined NTP servers** at each regional hub
- **PTP (IEEE 1588)** for microsecond-level sync where required
- **Flink watermark strategy**: Late events accepted within 120s window; events beyond watermark logged and processed in next batch window
- **Timestamp provenance**: Each event carries both device-reported time and network-receipt time; discrepancy > 60s triggers a timestamp anomaly alert

---

## 9. Security Architecture

### 9.1 Zero Trust Model

- **No implicit trust** based on network location
- Every service call requires a SPIFFE X.509 SVID (workload identity)
- Istio enforces mTLS for all east-west service traffic
- All north-south traffic terminated at Kong API gateway with OAuth 2.0 + JWT

### 9.2 Data Encryption

| Tier | Encryption |
|---|---|
| Data in transit | TLS 1.3 minimum; mTLS for internal services |
| Data at rest | AES-256-GCM; LUKS full-disk encryption on edge nodes |
| Key management | HashiCorp Vault with HSM (Hardware Security Module) backing |
| Database | Neo4j native encryption; TDE for PostgreSQL |
| Video clips | Client-side encryption before upload to MinIO/S3 |

### 9.3 Access Control

- **RBAC**: Roles defined at platform level (Analyst, Senior Analyst, System Admin, Auditor)
- **ABAC**: Attribute-based policies for data classification (RESTRICTED vs SECRET data requires clearance attribute)
- **OPA (Open Policy Agent)**: Policy-as-code for fine-grained authorization; evaluated at API gateway and service mesh
- **Multi-factor authentication**: FIDO2/WebAuthn for all analyst accounts
- **Session management**: 8-hour session max; re-authentication for high-risk operations

### 9.4 Audit and Forensics

- **Immutable audit log**: All queries, alerts, data access, and analyst actions written to tamper-proof log (WORM storage)
- **Non-repudiation**: Digital signatures on all alert escalations
- **Data lineage**: Every graph node traces back to source event, model version, and analyst action

### 9.5 Network Segmentation

```
[Internet / Source feeds] → [DMZ / Ingestion Zone]
[Ingestion Zone] → [Processing Zone] (Kafka, Flink)
[Processing Zone] → [Intelligence Zone] (Graph DB, ML)
[Intelligence Zone] → [Analyst Zone] (Dashboards, APIs)
```
Firewall rules permit only specific port/protocol flows between zones. No lateral movement possible without explicit allow rules.

---

## 10. Real-Time vs Batch Processing

### 10.1 Real-Time Pipeline (< 5 seconds end-to-end)

**Use cases**: Face match alert, abandoned object detection, live threat correlation, cyber intrusion detection

**Path**:
```
Edge inference → Kafka → Flink CEP (Complex Event Processing) → Ontology write → Rule engine → Alert push
```

**Flink CEP patterns** detect:
- Person A (watchlist) appears at Location X → immediate alert
- 3+ watchlist persons converge within 500m over 10 minutes → alert
- Cyber intrusion + simultaneous physical access event at same facility → critical alert

### 10.2 Near-Real-Time Pipeline (5s – 5 minutes)

**Use cases**: SOCMINT cross-domain correlation, entity enrichment, dynamic risk score updates

**Path**:
```
Kafka → Flink (stateful joins, enrichment) → Graph update → Score propagation
```

### 10.3 Batch Pipeline (minutes – hours)

**Use cases**: GNN model retraining, historical campaign analysis, ontology versioning, compliance reporting

**Path**:
```
Airflow DAG → Spark job → Iceberg tables → Graph bulk update → MLflow model push
```

### 10.4 Lambda Architecture Summary

| Processing Type | Latency | Technology | Output |
|---|---|---|---|
| Speed layer | < 5s | Flink + Redis | Real-time alerts |
| Serving layer | 5s–5min | Flink stateful + Neo4j | Enriched graph views |
| Batch layer | hours–days | Spark + Iceberg | Retrained models, reports |

---

## 11. Deployment Architecture — Edge + Cloud Hybrid

### 11.1 Edge Node Hardware Tiers

| Tier | Hardware | Power | Use Case |
|---|---|---|---|
| High-end edge | NVIDIA Jetson Orin NX (16GB) | 10–25W | Multi-camera, face recognition, HD inference |
| Mid-range edge | Intel NUC / mini-PC with iGPU | 15–65W | 2–4 cameras, basic detection |
| Low-power edge | Raspberry Pi CM4 + Hailo-8 accelerator | 5–8W | 1–2 cameras, crowd density only |
| Vehicle-mounted | Jetson Orin + LTE modem + battery | Battery-backed | Mobile surveillance units |

### 11.2 Cloud Infrastructure Zones

```
Primary DC (Government data center or sovereign cloud — e.g., NIC Cloud / AWS GovCloud)
├── Kubernetes cluster: Intelligence Core (Neo4j, Kafka, Flink, ML serving)
├── GPU cluster: Model training and batch inference
└── Cold storage: Iceberg + S3-compatible store

Secondary DC (Disaster Recovery — separate region)
├── Neptune replica for SPARQL queries
├── Kafka MirrorMaker replica
└── Pre-warmed K8s cluster (cold standby, 15-min RTO)

Edge clusters (K3s) in 20 regional hubs
└── Kafka regional brokers
└── Regional graph partition (Neo4j shard)
└── Regional Flink cluster
```

### 11.3 Connectivity Design

- **Primary link**: MPLS leased lines between regional hubs and DC
- **Backup link**: 4G LTE / 5G with VPN
- **Edge → Hub**: MQTT over TLS (low bandwidth) or gRPC (high bandwidth)
- **Offline mode**: Edge nodes operate fully locally for up to 72h; sync on reconnection

---

## 12. India-Specific Deployment Constraints

### Constraint 1: Camera Compatibility (30% proprietary firmware)

**Why it happens**: India's surveillance camera market has significant penetration of low-cost Chinese OEM cameras (Hikvision, Dahua clones, local brands) with non-standard RTSP implementations or locked ONVIF profiles. Many government-procured cameras predate open standards.

**Impact**: 30% of cameras cannot be read via standard RTSP/ONVIF pull. If unconstrained, this creates a significant coverage gap in high-value locations.

**Solutions**:
- Deploy **RTSP proxy adapters** (FFmpeg-based microservice) at edge that supports vendor-specific authentication schemes and SDK access (Hikvision SDK, Dahua NetSDK)
- Maintain a **camera compatibility matrix** database with per-model adapter configuration
- For truly incompatible cameras: deploy a **frame-capture relay box** (Raspberry Pi with HDMI capture card) physically tapped into camera's local monitor output
- **Standard procurement policy**: All new camera purchases require ONVIF Profile S compliance as a contractual condition
- **Long-term**: Replace 30% incompatible cameras over 3-year phased program

### Constraint 2: Two-Way Camera Control (40% read-only)

**Why it happens**: Many cameras, especially older government-installed units, expose only unidirectional RTSP streams. PTZ control, zoom, and configuration changes require physical access or proprietary NVR software not exposed via API.

**Impact**: System cannot redirect camera to track a detected threat, reducing real-time response capability.

**Solutions**:
- Design the system with **passive observability as the default** — do not depend on PTZ control for core function
- Where PTZ is available, implement **ONVIF PTZ service** wrappers in the camera adapter service
- For read-only cameras covering critical zones, deploy **overlapping camera placement** to achieve 360° coverage without PTZ
- **Auto-track workaround**: Track subject across multiple fixed cameras using cross-camera re-identification (ReID models) rather than PTZ

### Constraint 3: Edge Device Power and Weatherproofing

**Why it happens**: India's distributed infrastructure — rural border zones, remote checkpoints, transport hubs in extreme climates — makes AC power and weatherproofed enclosures economically unfeasible for all locations.

**Impact**: Edge nodes may experience frequent power cycles, data corruption on unclean shutdown, hardware failure from heat/dust/moisture.

**Solutions**:
- **Power**: UPS battery backup (minimum 4h) + solar panel option for off-grid locations; Raspberry Pi CM4 (5W) as baseline hardware
- **Weatherproofing**: IP67-rated enclosures (Spelsberg, Rittal clones) for outdoor edge nodes; active cooling only where power budget allows; passive heat sinks otherwise
- **Unclean shutdown protection**: Overlay filesystem (OverlayFS) on read-only base OS; all state stored in RocksDB on a separate partition with journaling
- **Hardware watchdog**: Kernel watchdog timer; auto-reboot on hang; systemd service auto-restart policies
- **Thermal throttling**: Models automatically downsampled (lower FPS, lower resolution) when device temperature exceeds 75°C

### Constraint 4: Bandwidth — Central System Overload

**Why it happens**: Naively streaming raw video or even raw metadata from 10,000+ nodes to a central system would require multi-Tbps uplinks. India's leased line infrastructure in tier-2/3 cities is limited and expensive.

**Impact**: Central system saturation, high latency, dropped events, inability to scale.

**Solutions**:
- **Metadata-only uplink**: Edge runs full inference locally; sends only structured event metadata (JSON, ~1KB/event) not video. Typical bandwidth: 10KB/s per camera vs 4Mbps for raw stream — 400x reduction
- **Video-on-demand**: Raw video clips only requested on alert escalation via pre-signed URL from MinIO; not streamed continuously
- **Hierarchical aggregation** (see Section 7.2): 50 edge nodes → 1 regional aggregator reduces per-connection fan-out
- **Adaptive sampling**: Edge nodes reduce FPS during low-activity periods (night, off-peak) using motion detection as a trigger gate
- **Compression**: Protobuf for event serialization (3–5x smaller than JSON); LZ4 compression on Kafka topics

### Constraint 5: Model Drift — Changing Environments

**Why it happens**: India's diverse geography, seasonal lighting changes (monsoon, summer glare), crowd composition shifts, and annual events (festivals, protests) cause distribution shift in visual data, degrading model accuracy over time.

**Impact**: Increasing false negatives — real threats missed. Increasing false positives — analyst alert fatigue.

**Solutions**:
- **Evidently AI drift monitoring**: Compare feature distributions of current inference inputs vs training data baseline. Alert when drift score (PSI or KL-divergence) exceeds threshold
- **Federated retraining**: Regional models retrained on locally-collected statistics without sending raw data to center, preserving privacy while adapting to local conditions
- **Continual learning**: A small supervised fine-tuning loop using analyst-confirmed true positives from the last 30 days as additional training signal
- **Seasonal model variants**: Maintain separate model checkpoints per season; swap automatically based on calendar + drift score
- **Shadow model testing**: New model versions run in parallel on live data before promotion; only promoted if precision/recall improves

### Constraint 6: False Positives — Multi-Source Fusion Increases Errors

**Why it happens**: Each source (CCTV, cyber, SOCMINT) independently generates alerts with their own false positive rates. Naive fusion multiplies these errors. A face recognition system at 95% accuracy still generates 500 false positives per 10,000 queries.

**Impact**: Alert fatigue paralyzes analysts; critical alerts buried in noise; potential civil liberties violations from acting on false data.

**Solutions**:
- **Bayesian evidence fusion**: Combine independent evidence using probabilistic graphical models (not simple AND/OR). An alert requires corroboration from ≥2 independent sources
- **Confidence thresholds by action tier**: Score 0.4–0.6 → analyst review only; 0.6–0.8 → soft alert + enrichment; >0.8 → escalation with supervisor confirmation required
- **Temporal cross-validation**: A SOCMINT signal + a CCTV sighting within 2h at the same location = corroborated alert; one signal alone = watchlist flag only
- **Feedback loop**: Analyst decisions (true positive / false positive) feed back into model scores and ontology weights
- **Audit trail**: Every fusion decision is logged with all contributing signals, enabling post-incident false positive analysis

### Constraint 7: Clock Synchronization — Inconsistent Timestamps

**Why it happens**: Edge devices in remote locations may not have reliable NTP access. Battery-backed RTCs drift. Some cameras have no RTC at all and reset to epoch on power cycle.

**Impact**: Temporal correlation fails — co-location events appear to not overlap; attack timelines reconstructed incorrectly; audit trails unreliable.

**Solutions**:
- **GPS receivers** on high-value edge nodes (±50ns accuracy); cost ~₹1,500–₹3,000 per unit
- **Stratum-1 NTP servers** at all regional aggregation hubs; edge nodes sync on every network reconnect
- **Timestamp anomaly detection**: Flink operator flags events where device_time diverges from server_receipt_time by >60s; these events use server_receipt_time with an anomaly flag
- **Camera firmware patching**: Automated NTP configuration push through camera management service (where ONVIF allows)
- **Relative ordering fallback**: For devices with unreliable clocks, use sequence numbers + relative timing between events rather than absolute timestamps

### Constraint 8: Edge Device Failure — Infrastructure Fragility

**Why it happens**: Power surges, rodent damage to cables, extreme heat, flooding, physical vandalism, and poor maintenance practices are common in India's diverse deployment environments.

**Impact**: Gaps in surveillance coverage; data loss; security blind spots exploited by adversaries.

**Solutions**:
- **Redundant coverage**: Camera placement design mandates ≥2 edge nodes covering any single critical zone; failure of one leaves ≥1 active
- **Automated failure detection**: K3s node heartbeat; if no heartbeat in 90s, alert ops team + flag coverage gap on dashboard
- **Rapid replacement**: Standardized edge node images on USB/SD cards; field technician can replace and boot a new node in <30 minutes with zero-touch provisioning (ignition + Kubernetes bootstrap)
- **Tamper detection**: Physical intrusion sensors on edge enclosures; unauthorized open triggers immediate security alert
- **Remote diagnostics**: SSH jump host through WireGuard VPN; no need for physical access for software issues

### Constraint 9: Social Media Noise — Unreliable and Noisy Signals

**Why it happens**: India's social media landscape includes massive volumes of misinformation, political hyperbole, bot networks, coordinated inauthentic behavior, and genuine citizen reporting all mixed together. SOCMINT from platforms like Twitter/X, Facebook, Telegram, and ShareChat is extremely noisy.

**Impact**: NLP models trained on Western text data perform poorly on Hinglish, regional scripts, and India-specific context. High noise-to-signal ratio overwhelms analysts.

**Solutions**:
- **Multilingual NER fine-tuned on Indian content**: IndicBERT + MuRIL models trained on Hindi, Telugu, Tamil, Urdu, Bengali, Marathi
- **Source credibility scoring**: Each social media account assigned a credibility score based on account age, follower graph, historical accuracy, network cluster membership
- **Bot detection**: Graph-based bot detection (accounts with similar post timing, content, network structure) using GNN classifier
- **Minimum corroboration rule**: No SOCMINT signal alone triggers an operational alert; must be corroborated by physical or cyber signal
- **Human SOCMINT analysts**: Dedicated analyst team for SOCMINT triage; AI surfaces candidates, humans validate
- **False positive buffer**: 48h hold on SOCMINT-sourced entity flags before escalation, allowing signal accumulation

### Constraint 10: Ontology Staleness — Static Graphs Become Outdated

**Why it happens**: New terrorist groups form, splinter, rebrand. New attack tactics emerge (e.g., drone-based attacks, new malware families). New geopolitical entities emerge. A static OWL ontology defined at deployment time will miss these.

**Impact**: System fails to model new threat types; entity resolution fails for newly-formed organizations; detection rules miss novel attack patterns.

**Solutions**:
- **Ontology versioning**: Git-based ontology management (OWL files in version control); all changes go through a review pipeline before deployment
- **Automated concept extraction**: NLP pipeline monitors threat intelligence reports (CERT-In, NSG advisories, open-source threat reports) and proposes new entity types and relationships to ontology maintainers
- **STIX/TAXII integration**: Automated ingestion of structured threat intelligence from CERT-In, NCIIPC, and international partners; new TTPs auto-mapped to MITRE ATT&CK and ontology
- **Ontology governance board**: Domain experts (security, cyber, legal) review and approve ontology changes quarterly
- **Backward compatibility**: All ontology changes maintain backward compatibility with existing graph data; migrations run as background Spark jobs

---

## 13. Trade-offs and Limitations

### Performance vs Privacy

Higher accuracy face recognition requires larger models and more data. Larger models need more edge hardware. More data collection increases privacy risk. **Decision**: Use face recognition only at explicitly designated checkpoints (airports, borders, critical infrastructure), not in general public spaces.

### Latency vs Accuracy

Real-time alerts (<5s) require smaller, faster models with lower accuracy. Higher accuracy requires larger models with 30–120s latency. **Decision**: Two-tier approach — fast low-accuracy alert triggers enrichment by slower high-accuracy model.

### Centralization vs Resilience

Centralized graph enables global correlation but is a single point of failure and a high-value target. Fully distributed graph loses global correlation ability. **Decision**: Tiered architecture with regional partitions that can operate independently.

### Coverage vs Cost

Full coverage of India's 14,000+ police stations, 7,000+ railway stations, and 500+ airports would require 500,000+ cameras and 50,000+ edge nodes — estimated ₹15,000–20,000 crore. **Decision**: Phased rollout starting with top 100 highest-risk locations.

### Automation vs Human Oversight

Higher automation reduces analyst workload but increases risk of unchecked false positives leading to wrongful targeting. **Decision**: Automation surfaces and scores; humans authorize all operational responses.

---

## 14. Cost Considerations

### Hardware (per edge node, estimated)

| Component | Cost (INR) |
|---|---|
| Jetson Orin NX 16GB | ₹45,000–60,000 |
| IP67 enclosure + mounting | ₹8,000–15,000 |
| UPS + battery pack (4h) | ₹5,000–10,000 |
| GPS module | ₹2,000–3,000 |
| LTE modem | ₹3,000–5,000 |
| Miscellaneous (cables, SSD) | ₹3,000–5,000 |
| **Total per node** | **~₹70,000–1,00,000** |

**1,000 high-end nodes**: ₹70–100 crore hardware cost.

### Cloud/DC Infrastructure (annual, estimated for 10,000 nodes)

| Component | Annual Cost (USD) |
|---|---|
| Compute (K8s cluster, 100 nodes) | $800,000–1,200,000 |
| GPU instances (model training) | $200,000–400,000 |
| Storage (S3 + NVMe) | $300,000–500,000 |
| Kafka/Flink managed services | $150,000–250,000 |
| Neo4j Enterprise license | $200,000–400,000 |
| Network / CDN / egress | $100,000–200,000 |
| **Total annual cloud** | **~$1.75M–2.95M** |

### Personnel (annual, India market rates)

| Role | Count | Annual Cost |
|---|---|---|
| ML/AI engineers | 10 | ₹3–5 Cr |
| Platform/DevOps engineers | 8 | ₹2–3 Cr |
| Security engineers | 5 | ₹1.5–2.5 Cr |
| Ontology/data engineers | 6 | ₹1.5–2 Cr |
| Analyst team | 20 | ₹2–3 Cr |
| **Total personnel** | **~49** | **~₹10–15 Cr/year** |

---

## 15. Ethical Concerns and Governance

### Privacy

- **Data minimization**: CCTV metadata (bounding boxes, not faces) is the default. Face biometrics collected only at designated checkpoints with legal authorization.
- **Retention limits**: Raw video: 72h. Metadata: 1 year. Watchlist-flagged data: duration of investigation + 2 years. All data purged per retention schedule.
- **Right to erasure**: Workflow for processing court-ordered data erasure requests.

### Surveillance Overreach

- **Geographic fencing**: System cannot be deployed in places of worship, hospitals, or private residences without specific court order.
- **Purpose limitation**: Data collected for counter-terrorism cannot be used for general law enforcement, tax enforcement, or political monitoring.
- **Independent oversight**: System logs accessible to a parliamentary oversight committee; quarterly audit by independent technical auditors.

### Algorithmic Bias

- **Demographic parity testing**: Face recognition models tested across skin tones, genders, and age groups. No model deployed with >5% disparity in false positive rate across demographic groups.
- **Training data diversity**: Models trained on India-specific datasets reflecting demographic diversity.
- **Bias incident response**: Process for investigating and remediating bias incidents within 48h.

### Misuse Prevention

- **Query audit**: Every graph query, alert, and data access is logged with user identity, timestamp, and purpose code.
- **Analyst access controls**: Analysts can only access data in their authorized geographic zone.
- **Whistleblower protection**: Secure internal channel for reporting misuse; external reporting to oversight body.
- **No automated arrest**: System never directly triggers law enforcement action. All responses require human authorization at the appropriate rank.

### Legal Framework (India-specific)

- Compliance with: IT Act 2000, PDPB/DPDP Act 2023, CrPC surveillance provisions, MHA/IB operational guidelines
- TRAI and DoT compliance for interception of communications
- Data localization: All data stored on India-territory infrastructure

---

## 16. Status and Roadmap

### Current Status

Early-stage foundation repository. Ontology schema defined, data contracts in progress, baseline pipeline architecture designed.

### Phase 1 — Foundation 
- Core ontology finalized and versioned
- Data ingestion pipelines for CCTV + cyber telemetry
- Edge node prototype (Jetson Orin + YOLOv8)
- Kafka + Flink stream processing backbone
- Neo4j graph store with basic CRUD
- Analyst dashboard prototype

### Phase 2 — Intelligence Core 
- Full ontology engine with SWRL rules
- Cross-domain correlation (CCTV + cyber)
- SOCMINT integration
- GNN risk scoring
- Alert management and case management integration
- Security architecture (zero trust, audit logging)

### Phase 3 — Scale and Harden 
- Scale to 1,000 edge nodes
- Federated model retraining
- Regional aggregation tier deployment
- Drift monitoring and automated retraining
- Full DR/BCP testing
- Penetration testing and security audit

### Phase 4 — Production Rollout 
- Scale to 10,000 edge nodes
- Multi-agency integration
- Legal compliance certification
- Operational handover to analyst teams
- Ongoing model governance program

---

