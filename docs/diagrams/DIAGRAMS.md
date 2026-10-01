# RetailNexus Data Lakehouse — Architecture Diagrams

All diagrams use [Mermaid](https://mermaid.js.org/) and render natively in GitHub.

---

## Diagram 1 — High-Level System Architecture

```mermaid
graph TB
    subgraph SOURCES["📦 Operational Source Systems"]
        EC["🛒 Ecommerce\n(Orders · Cart · Clickstream)"]
        ST["🏪 Physical Stores\n(POS · Transactions)"]
        WH["🏭 Warehouse\n(WMS · IoT Sensors)"]
        FIN["💰 Finance\n(ERP · GL · AR/AP)"]
        CRM["👥 CRM\n(Customers · Support)"]
        MKT["📣 Marketing\n(Campaigns · Attribution)"]
        INV["📦 Inventory\n(Stock · Suppliers)"]
        HR["👤 HR\n(Employees)"]
    end

    subgraph INGEST["⬇️ Ingestion Layer"]
        BATCH["Batch Ingestion\nCSV · JSON · Parquet\nREST API · CDC"]
        STREAM["Kafka Streaming\n5 Topics\n30s micro-batch"]
        FILE["File Watcher\nS3 Event Trigger"]
    end

    subgraph KAFKA["📨 Apache Kafka"]
        K1["retailnexus.orders"]
        K2["retailnexus.payments"]
        K3["retailnexus.clickstream"]
        K4["retailnexus.inventory"]
        K5["retailnexus.iot-sensors"]
    end

    subgraph LAKEHOUSE["🏔️ Delta Lakehouse (AWS S3)"]
        BRONZE["🥉 BRONZE LAYER\nRaw · Immutable · Append-only\nDelta Lake tables\n7-year retention"]
        SILVER["🥈 SILVER LAYER\nCleansed · Validated · Deduplicated\nSCD Type 2 · PII Tokenized\nBusiness rules enforced"]
        GOLD["🥇 GOLD LAYER\nDimensional Models\n6 Facts · 8 Dimensions\nPre-aggregated KPIs"]
    end

    subgraph SPARK["⚡ Apache Spark (EMR)"]
        SB["Bronze Spark Jobs"]
        SS["Silver Spark Jobs"]
        SG["Gold Spark Jobs"]
        SK["Structured Streaming"]
    end

    subgraph ORCHESTRATION["🎛️ Apache Airflow"]
        DAG1["bronze_batch_ingestion\n0 * * * *"]
        DAG2["silver_processing\n30 * * * *"]
        DAG3["gold_processing\n0 3 * * *"]
        DAG4["warehouse_load\n0 5 * * *"]
        DAG5["quality_checks\n0 6 * * *"]
    end

    subgraph QUALITY["✅ Data Quality"]
        DQ["80+ Automated Checks\nNull · Dupe · Range\nReferential · Freshness"]
        QR["Quality Reports\nPrometheus Metrics\nGrafana Dashboard"]
    end

    subgraph WAREHOUSES["🏛️ Data Warehouses"]
        RS["Amazon Redshift\nBI & Reporting"]
        SF["Snowflake\nSelf-Service Analytics"]
        BQ["Google BigQuery\nML & Data Science"]
    end

    subgraph SERVING["🚀 Data Serving"]
        API["FastAPI\n/v1/sales · /v1/customers\n/v1/inventory · /v1/products"]
        BI["BI Tools\nTableau · Power BI · Looker"]
        ML["ML Pipelines\nFeature Store\nModel Training"]
        NB["Jupyter Notebooks\nAd-hoc Analysis"]
    end

    subgraph OBSERVE["👁️ Observability"]
        PROM["Prometheus\nMetrics Collection"]
        GRAF["Grafana\nDashboards · Alerts"]
        ELK["ELK Stack\nElastic · Logstash · Kibana"]
    end

    SOURCES --> BATCH
    SOURCES --> KAFKA
    KAFKA --> STREAM
    BATCH --> SB
    STREAM --> SK
    FILE --> SB

    SB --> BRONZE
    SK --> BRONZE

    DAG1 --> SB
    DAG2 --> SS
    DAG3 --> SG

    BRONZE --> SS
    SS --> SILVER
    SILVER --> DQ
    DQ --> QR
    DQ --> SG
    SG --> GOLD

    DAG4 --> WAREHOUSES
    DAG5 --> DQ

    GOLD --> RS
    GOLD --> SF
    GOLD --> BQ
    GOLD --> API
    GOLD --> ML

    RS --> BI
    API --> BI

    SB --> PROM
    SS --> PROM
    SG --> PROM
    API --> PROM
    PROM --> GRAF
    INGEST --> ELK
    SPARK --> ELK
    API --> ELK
```

---

## Diagram 2 — Medallion Data Flow (Detailed)

```mermaid
flowchart LR
    subgraph SRC["Source Systems"]
        direction TB
        S1["Ecommerce API"]
        S2["PostgreSQL CDC"]
        S3["CSV Files"]
        S4["Kafka Topics"]
        S5["REST APIs"]
    end

    subgraph BRONZE["🥉 Bronze"]
        direction TB
        B1["Raw Delta Tables\n+ _ingested_at\n+ _source_system\n+ _pipeline_run_id\n+ _schema_version"]
        B2["Schema Validation\n(strict reject)"]
        B3["Metadata Enrichment"]
    end

    subgraph SILVER["🥈 Silver"]
        direction TB
        SI1["Type Casting\n& Normalisation"]
        SI2["Null Handling\n(impute/drop/flag)"]
        SI3["Deduplication\n(composite key)"]
        SI4["Business Rules\nValidation"]
        SI5["PII Tokenisation"]
        SI6["SCD Type 2 Merge\n(dimensions only)"]
    end

    subgraph QG["Quality Gate"]
        QC["80+ DQ Checks\n────────────\nCRITICAL → halt\nHIGH → flag\nMEDIUM → warn"]
    end

    subgraph GOLD["🥇 Gold"]
        direction TB
        G1["fact_sales"]
        G2["fact_inventory"]
        G3["fact_finance"]
        G4["dim_customer\n(SCD2)"]
        G5["dim_product\n(SCD2)"]
        G6["dim_date"]
        G7["KPI Summaries"]
    end

    subgraph CONSUME["Consumers"]
        direction TB
        C1["Redshift\n(BI / Reporting)"]
        C2["Snowflake\n(Self-Service)"]
        C3["BigQuery\n(ML / DS)"]
        C4["FastAPI\n(Applications)"]
    end

    SRC --> B1
    B1 --> B2
    B2 --> B3
    B3 --> SI1
    SI1 --> SI2
    SI2 --> SI3
    SI3 --> SI4
    SI4 --> SI5
    SI5 --> SI6
    SI6 --> QC
    QC -->|PASS| GOLD
    QC -->|CRITICAL FAIL| X["❌ Halt Pipeline\nAlert On-Call"]
    G1 & G2 & G3 & G4 & G5 & G6 & G7 --> CONSUME
```

---

## Diagram 3 — Streaming Architecture

```mermaid
flowchart TD
    subgraph PRODUCERS["Event Producers"]
        P1["Ecommerce App\n(Orders · Cart)"]
        P2["Payment Service\n(Transactions)"]
        P3["Web/App\n(Clickstream)"]
        P4["WMS\n(Inventory)"]
        P5["IoT Devices\n(Sensors)"]
    end

    subgraph KAFKA["Apache Kafka Cluster (3 Brokers)"]
        K1["orders\nPartitions: 12\nRetention: 7d"]
        K2["payments\nPartitions: 12\nRetention: 7d"]
        K3["clickstream\nPartitions: 24\nRetention: 3d"]
        K4["inventory\nPartitions: 6\nRetention: 7d"]
        K5["iot-sensors\nPartitions: 24\nRetention: 1d"]
    end

    subgraph SPARK_STREAM["Spark Structured Streaming (EMR)"]
        SR["readStream\n.format('kafka')"]
        SW["Watermark\n10-min late tolerance"]
        ST["Schema Validation\n+ Transform"]
        AGG["Stateful Aggregations\n(Session / Tumbling Windows)"]
        CHK["Checkpoint\nS3-backed\nevery 5 min"]
    end

    subgraph BRONZE_STREAM["Bronze Streaming Tables"]
        BT1["bronze.orders_stream\n(append-only Delta)"]
        BT2["bronze.payments_stream"]
        BT3["bronze.clickstream"]
        BT4["bronze.inventory_stream"]
        BT5["bronze.iot_sensors"]
    end

    subgraph SILVER_TRIGGER["Silver Triggered Job (every 5 min)"]
        SV["readStream\n(CDF-based)\nMERGE → Silver"]
    end

    subgraph REALTIME["Near-Real-Time Gold"]
        RT1["Current Inventory\n(per SKU / location)"]
        RT2["Live Order Status"]
        RT3["Active Sessions"]
    end

    P1 -->|Avro| K1
    P2 -->|Avro| K2
    P3 -->|JSON| K3
    P4 -->|Avro| K4
    P5 -->|JSON| K5

    K1 & K2 & K3 & K4 & K5 --> SR
    SR --> SW
    SW --> ST
    ST --> AGG
    AGG --> CHK
    AGG --> BT1 & BT2 & BT3 & BT4 & BT5
    BT1 & BT2 & BT3 & BT4 & BT5 --> SV
    SV --> RT1 & RT2 & RT3
```

---

## Diagram 4 — Dimensional Model (Star Schema)

```mermaid
erDiagram
    dim_date {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        int week
        int day_of_week
        boolean is_holiday
        boolean is_weekend
        string fiscal_year
        string fiscal_quarter
    }

    dim_customer {
        int customer_key PK
        string customer_id NK
        string full_name
        string email_token
        string segment
        string loyalty_tier
        string country_code
        string city
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_product {
        int product_key PK
        string sku NK
        string product_name
        string category_l1
        string category_l2
        string brand
        decimal cost_price
        decimal list_price
        string supplier_id
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_store {
        int store_key PK
        string store_id NK
        string store_name
        string store_type
        string city
        string region
        string country
        int employee_count
        decimal sq_footage
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    dim_promotion {
        int promotion_key PK
        string promotion_id NK
        string promotion_name
        string promotion_type
        decimal discount_pct
        date start_date
        date end_date
    }

    dim_location {
        int location_key PK
        string location_id NK
        string location_type
        string city
        string state
        string country
        string region
        decimal latitude
        decimal longitude
    }

    fact_sales {
        bigint sales_key PK
        int date_key FK
        int customer_key FK
        int product_key FK
        int store_key FK
        int promotion_key FK
        string order_id
        string order_line_id
        string channel
        int quantity
        decimal unit_price
        decimal discount_amount
        decimal net_revenue
        decimal gross_margin
        decimal tax_amount
        string order_status
    }

    fact_inventory {
        bigint inventory_key PK
        int date_key FK
        int product_key FK
        int location_key FK
        int store_key FK
        int quantity_on_hand
        int quantity_reserved
        int quantity_available
        int reorder_point
        decimal inventory_value
        int days_of_supply
    }

    fact_returns {
        bigint return_key PK
        int date_key FK
        int customer_key FK
        int product_key FK
        int store_key FK
        string order_id
        string return_id
        string return_reason
        int quantity_returned
        decimal refund_amount
    }

    fact_shipments {
        bigint shipment_key PK
        int date_key FK
        int location_key FK
        string order_id
        string shipment_id
        string carrier
        string status
        date estimated_delivery
        date actual_delivery
        int transit_days
        decimal shipping_cost
    }

    dim_date ||--o{ fact_sales : "date_key"
    dim_customer ||--o{ fact_sales : "customer_key"
    dim_product ||--o{ fact_sales : "product_key"
    dim_store ||--o{ fact_sales : "store_key"
    dim_promotion ||--o{ fact_sales : "promotion_key"
    dim_date ||--o{ fact_inventory : "date_key"
    dim_product ||--o{ fact_inventory : "product_key"
    dim_location ||--o{ fact_inventory : "location_key"
    dim_date ||--o{ fact_returns : "date_key"
    dim_customer ||--o{ fact_returns : "customer_key"
    dim_product ||--o{ fact_returns : "product_key"
    dim_date ||--o{ fact_shipments : "date_key"
    dim_location ||--o{ fact_shipments : "location_key"
```

---

## Diagram 5 — Airflow DAG Dependencies

```mermaid
graph LR
    subgraph DAILY["Daily Pipeline (runs 00:00–07:00 UTC)"]
        direction LR
        A1["bronze_batch_ingestion\n0:00 UTC\nSLA: 45min"]
        A2["silver_processing\n0:30 UTC\nSLA: 45min"]
        A3["gold_processing\n3:00 UTC\nSLA: 2h"]
        A4["warehouse_load\n5:00 UTC\nSLA: 1h"]
        A5["quality_checks\n6:00 UTC\nSLA: 30min"]
        A6["quality_report\n7:00 UTC\nSLA: 15min"]
        A7["stream_monitor\n*/5 min\nContinuous"]
    end

    subgraph WEEKLY["Weekly Maintenance (Sunday 2:00 UTC)"]
        B1["compaction\nDelta OPTIMIZE\n+ VACUUM"]
    end

    subgraph MANUAL["Manual / Backfill"]
        C1["backfill_coordinator\nParameterized\ndate range"]
    end

    A1 -->|ExternalTaskSensor| A2
    A2 -->|ExternalTaskSensor| A3
    A2 -->|ExternalTaskSensor| A5
    A3 -->|ExternalTaskSensor| A4
    A5 -->|ExternalTaskSensor| A6

    A1 -.->|SLA Miss| ALERT["📢 PagerDuty\n+ Slack Alert"]
    A2 -.->|SLA Miss| ALERT
    A3 -.->|SLA Miss| ALERT
    A4 -.->|SLA Miss| ALERT
    A5 -.->|Quality CRITICAL| ALERT

    C1 -->|triggers| A1
```

---

## Diagram 6 — CI/CD Pipeline

```mermaid
flowchart LR
    subgraph DEV["Developer Workflow"]
        GIT["git push\nfeature branch"]
    end

    subgraph CI["CI Pipeline (Every PR)"]
        direction TB
        L1["ruff lint\n+ isort check"]
        L2["mypy\ntype checking"]
        L3["pytest\nunit tests"]
        L4["pytest\nintegration tests"]
        L5["docker build\n(all services)"]
        L6["coverage report\n> 90% required"]
        L1 --> L2 --> L3 --> L4 --> L5 --> L6
    end

    subgraph STG["Staging Deploy (merge to main)"]
        direction TB
        S1["Build & push\nDocker images\n(ECR)"]
        S2["Deploy to\nstaging ECS"]
        S3["Smoke tests\n+ integration suite"]
        S4["Notify team\n(Slack)"]
        S1 --> S2 --> S3 --> S4
    end

    subgraph PROD["Prod Deploy (release tag v*)"]
        direction TB
        P1["Build & push\nDocker images\n(ECR)"]
        P2["Terraform plan\n+ apply (infra)"]
        P3["Blue-green deploy\n(ECS Fargate)"]
        P4["Health check\n+ rollback gate"]
        P5["Notify team\n(PagerDuty + Slack)"]
        P1 --> P2 --> P3 --> P4 --> P5
    end

    GIT --> CI
    CI -->|all checks pass| STG
    STG -->|release tag| PROD
    CI -->|any check fails| FAIL["❌ Block merge\nNotify author"]
```

---

## Diagram 7 — Security & Data Governance

```mermaid
flowchart TD
    subgraph SOURCES["Source Data"]
        RAW["Raw Data\n(may contain PII/PCI)"]
    end

    subgraph BRONZE["Bronze Layer"]
        B_RAW["Stored as-is\nEncrypted at rest\n(S3 SSE-S3)"]
        B_ACL["Access: Platform Engineers\n+ Compliance (read-only)"]
    end

    subgraph SILVER["Silver Layer"]
        S_TOK["PII Tokenised\n• email → SHA-256\n• customer_id → UUID token\n• address → removed"]
        S_MASK["PCI Masked\n• card BIN only\n• no CVV/full PAN"]
        S_ACL["Access: Analysts\n+ Data Scientists (read-only)"]
    end

    subgraph GOLD["Gold Layer"]
        G_AGG["Aggregated / Anonymised\nNo individual PII\nin fact tables"]
        G_ACL["Access: BI Tools · API · ML\n(read-only, scoped)"]
    end

    subgraph SECURITY["Security Controls"]
        direction LR
        SM["AWS Secrets Manager\nAll credentials"]
        TLS["TLS 1.3\nAll inter-service"]
        IAM["IAM Least Privilege\nPer-component roles"]
        AUDIT["Audit Logging\n(CloudTrail + Elasticsearch)"]
        RBAC["RBAC\n(Airflow · Redshift · Snowflake)"]
    end

    RAW --> B_RAW
    B_RAW --> B_ACL
    B_ACL --> S_TOK
    S_TOK --> S_MASK
    S_MASK --> S_ACL
    S_ACL --> G_AGG
    G_AGG --> G_ACL

    SECURITY -.->|governs| BRONZE
    SECURITY -.->|governs| SILVER
    SECURITY -.->|governs| GOLD
```

---

## Diagram 8 — Monitoring & Observability Stack

```mermaid
graph TB
    subgraph INSTRUMENTATION["Instrumentation"]
        I1["PySpark Jobs\n(Prometheus Pushgateway)"]
        I2["FastAPI\n(prometheus-fastapi-instrumentator)"]
        I3["Airflow\n(StatsD → statsd_exporter)"]
        I4["Kafka\n(JMX → jmx_exporter)"]
    end

    subgraph METRICS["Metrics Pipeline"]
        PG["Prometheus\nPushgateway"]
        PROM["Prometheus Server\nScrape interval: 15s"]
        GRAF["Grafana\nDashboards + Alerts"]
    end

    subgraph LOGGING["Log Pipeline"]
        BEATS["Filebeat\n(all containers)"]
        LS["Logstash\nParse + Enrich"]
        ES["Elasticsearch\n30-day retention"]
        KIB["Kibana\nDashboards"]
    end

    subgraph ALERTS["Alert Routing"]
        AM["Alertmanager"]
        PD["PagerDuty\n(CRITICAL/HIGH)"]
        SL["Slack\n(HIGH/MEDIUM)"]
        EM["Email\n(summaries)"]
    end

    I1 --> PG
    I2 --> PROM
    I3 --> PROM
    I4 --> PROM
    PG --> PROM
    PROM --> GRAF
    PROM --> AM
    AM --> PD
    AM --> SL
    AM --> EM

    I1 & I2 & I3 & I4 --> BEATS
    BEATS --> LS
    LS --> ES
    ES --> KIB
```

---

## Diagram 9 — Local Development Stack (Docker Compose)

```mermaid
graph TB
    subgraph DOCKER["Docker Compose Network: lakehouse-net"]
        subgraph COMPUTE["Compute"]
            SP_M["spark-master\n:4040 :7077"]
            SP_W["spark-worker\n1 replica"]
        end

        subgraph MESSAGING["Messaging"]
            ZK["zookeeper\n:2181"]
            KF["kafka\n:9092"]
        end

        subgraph ORCHESTRATION["Orchestration"]
            AW["airflow-webserver\n:8080"]
            AS["airflow-scheduler"]
            AWORKER["airflow-worker"]
        end

        subgraph STORAGE["Storage"]
            PG["postgres\n:5432\n(Airflow + App DB)"]
            MN["minio\n:9000 :9001\n(S3 emulation)"]
        end

        subgraph API_SVC["API"]
            FA["fastapi\n:8000"]
        end

        subgraph MONITORING["Monitoring"]
            PR["prometheus\n:9090"]
            GR["grafana\n:3000"]
        end

        subgraph LOGGING_SVC["Logging"]
            EL["elasticsearch\n:9200"]
            LG["logstash\n:5044"]
            KB["kibana\n:5601"]
        end
    end

    SP_W -->|connects to| SP_M
    ZK --> KF
    AW & AS & AWORKER -->|metadata| PG
    FA -->|reads| PG
    FA -->|reads| MN
    SP_M -->|reads/writes| MN
    PR -->|scrapes| SP_M & KF & AW & FA
    GR -->|queries| PR
    EL --> KB
    LG --> EL

    DEV["👩‍💻 Developer\nlocalhost"] -->|":8080"| AW
    DEV -->|":4040"| SP_M
    DEV -->|":3000"| GR
    DEV -->|":8000"| FA
    DEV -->|":5601"| KB
    DEV -->|":9001"| MN
```

---

## Diagram 10 — Sequence: End-to-End Order Event

```mermaid
sequenceDiagram
    actor Customer
    participant EC as Ecommerce API
    participant KF as Kafka
    participant SS as Spark Streaming
    participant BZ as Bronze (Delta)
    participant SV as Silver (Delta)
    participant GD as Gold (Delta)
    participant API as FastAPI
    participant BI as Grafana Dashboard

    Customer->>EC: Place order
    EC->>KF: Produce OrderEvent (Avro)
    Note over KF: Topic: retailnexus.orders<br/>Partition: hash(order_id)

    KF->>SS: Micro-batch (30s)
    SS->>SS: Schema validate + watermark
    SS->>BZ: Append to bronze.orders_stream
    Note over BZ: ACID write<br/>Exactly-once guaranteed

    SS->>SV: Triggered Silver job (5 min)
    SV->>SV: Dedup + type cast + business rules
    SV->>SV: MERGE into silver.orders (upsert)
    Note over SV: DQ gate: 12 checks run

    SV->>GD: Near-RT Gold aggregation
    GD->>GD: Update current_order_status<br/>Recalculate daily_revenue_summary

    API->>GD: Query /v1/sales/latest
    GD-->>API: Current KPIs
    API-->>BI: JSON response < 50ms

    BI-->>Customer: Dashboard updated
    Note over BI: End-to-end latency: < 5 min
```
