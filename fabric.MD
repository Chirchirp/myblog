# Microsoft Fabric Medallion Architecture: Enterprise Implementation Guide & Operational Runbook

**Version:** 1.0
**Last Updated:** March 2026
**Audience:** Data Engineers, Data Analysts, BI Developers, Data Architects
**Prerequisites:** Familiarity with SQL, Spark, Data Modeling, and Azure Fundamentals

---

## Table of Contents

1. [Executive Summary & Learning Objectives](#1-executive-summary--learning-objectives)
2. [Recommended Architecture Overview](#2-recommended-architecture-overview)
3. [Fabric Fundamentals Required for This Guide](#3-fabric-fundamentals-required-for-this-guide)
4. [Workspace, Environment & Naming Conventions](#4-workspace-environment--naming-conventions)
5. [Ingestion Layer](#5-ingestion-layer)
6. [Bronze Layer (Raw)](#6-bronze-layer-raw)
7. [Silver Layer (Cleansed & Conformed)](#7-silver-layer-cleansed--conformed)
8. [Gold Layer (Business-Ready)](#8-gold-layer-business-ready)
9. [Warehouse Design & Semantic Model](#9-warehouse-design--semantic-model)
10. [Power BI Consumption](#10-power-bi-consumption)
11. [Security & Governance](#11-security--governance)
12. [CI/CD & Automation](#12-cicd--automation)
13. [Monitoring, Observability & Cost Control](#13-monitoring-observability--cost-control)
14. [Operational Runbooks & Playbooks](#14-operational-runbooks--playbooks)
15. [Templates & Artifacts](#15-templates--artifacts)
16. [Knowledge Check / Exercises](#16-knowledge-check--exercises)
17. [Appendix](#17-appendix)
18. [1-Page Quickstart](#18-1-page-quickstart)
19. [Exportable Checklist](#19-exportable-checklist)

---

## 1. Executive Summary & Learning Objectives

### 1.1 Executive Summary

This document provides a comprehensive, production-ready framework for implementing the Medallion (Bronze-Silver-Gold) data architecture on Microsoft Fabric. Based on Microsoft's recommended patterns , this guide transforms architectural theory into operational reality. It addresses the full lifecycle: from source ingestion through to Power BI consumption, with enterprise-grade governance, CI/CD, and cost optimization.

The Medallion architecture on Fabric leverages OneLake as a single logical data lake, Delta Lake for ACID transactions, and a unified compute layer that supports T-SQL, Spark, and real-time analytics . This guide assumes you're an experienced practitioner and focuses on the "how" and "why" of enterprise implementation.

### 1.2 Learning Objectives

After completing this guide, you will be able to:

- Design and implement a production-grade Medallion architecture on Microsoft Fabric
- Build automated ingestion pipelines from diverse sources (OLTP, SaaS, files, streams)
- Implement Bronze/Silver/Gold layers with proper data quality, governance, and lineage
- Create star-schema Gold models optimized for Power BI consumption
- Leverage Direct Lake mode for high-performance analytics on large datasets
- Implement enterprise security (RBAC, RLS, data masking) across the platform
- Establish CI/CD pipelines for Fabric artifacts using Deployment Pipelines and Git integration
- Monitor and optimize Fabric capacity costs using proven patterns
- Troubleshoot common issues with systematic runbooks

---

## 2. Recommended Architecture Overview

### 2.1 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                 │
├─────────────┬───────────────┬──────────────────┬───────────────────────────┤
│  OLTP       │  SaaS/APIs    │  Event Streams   │  Files                    │
│  (SQL,Oracle│  (SFDC,WD)    │  (Kafka, EH)     │  (SFTP, Blob)             │
└─────────────┴───────────────┴──────────────────┴───────────────────────────┘
                                      │
                      ┌───────────────┴───────────────┐
                      │         INGESTION LAYER        │
                      │  • Dataflows Gen2              │
                      │  • Pipelines                   │
                      │  • Eventstream                 │
                      │  • Shortcuts                   │
                      └───────────────┬───────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (OneLake)                              │
│                     (Lakehouse: bronze_ws)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Raw, immutable data in Delta format                                      │
│  • Time-partitioned (yyyy/MM/dd)                                           │
│  • Metadata: _source_system, _ingestion_ts, _batch_id                      │
│  • Full auditability, point-in-time recovery                               │
│                                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                        │
│  │ bronze_sales │ │ bronze_cust  │ │ bronze_prod  │                        │
│  │ (Delta)      │ │ (Delta)      │ │ (Delta)      │                        │
│  └──────────────┘ └──────────────┘ └──────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              │         TRANSFORMATION (Spark/Dataflows)       │
              │  • Deduplication     • Type casting           │
              │  • Standardization   • Data quality checks    │
              │  • Reference data joins                       │
              └───────────────────────┬───────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (OneLake)                              │
│                     (Lakehouse: silver_ws)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Cleansed, conformed, enriched data                                       │
│  • Enterprise-wide dimensions (Customer, Product, Date)                    │
│  • SCD Type 2 for historical tracking                                      │
│  • Data quality validated                                                  │
│                                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                        │
│  │ silver_orders│ │ dim_customer │ │ dim_product  │                        │
│  │ (Delta)      │ │ (SCD2)       │ │ (Delta)      │                        │
│  └──────────────┘ └──────────────┘ └──────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              │        BUSINESS TRANSFORMATIONS                │
              │  • Star schema modeling                        │
              │  • Aggregations                               │
              │  • Business logic & KPI calculations          │
              └───────────────────────┬───────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (OneLake)                               │
│                     (Lakehouse: gold_ws)                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Curated, business-ready star schemas                                     │
│  • Aggregated tables for performance                                       │
│  • Descriptive column names                                                │
│  • Pre-calculated measures                                                │
│                                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                        │
│  │ fact_sales   │ │ dim_customer │ │ agg_daily    │                        │
│  │ (star)       │ │ (conformed)  │ │ (aggregate)  │                        │
│  └──────────────┘ └──────────────┘ └──────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              │            SERVING & SEMANTIC LAYER            │
              │  ┌─────────────────┐ ┌─────────────────┐      │
              │  │  SQL Analytics  │ │  Semantic Model │      │
              │  │  Endpoint       │ │  (Power BI)     │      │
              │  └─────────────────┘ └─────────────────┘      │
              └───────────────────────┬───────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CONSUMPTION LAYER                                   │
│              ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│              │ Power BI     │ │ Excel        │ │ Custom Apps  │            │
│              │ (Direct Lake)│ │ (Analyze)    │ │ (GraphQL API)│            │
│              └──────────────┘ └──────────────┘ └──────────────┘            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Architecture Justification

The Medallion architecture is Microsoft's recommended design pattern for Fabric  because it provides:

| Layer | Purpose | Benefits |
|-------|---------|----------|
| **Bronze** | Raw data landing zone | Immutable audit trail, full data fidelity, point-in-time recovery via Delta time travel |
| **Silver** | Cleansed & conformed | Single source of truth, enterprise-wide consistency, reusability |
| **Gold** | Business-ready | Optimized for consumption, star schemas, aggregated for performance |

**Key Design Decisions:**
- **One Lakehouse per layer**: Each zone gets its own Lakehouse for logical separation 
- **Delta Lake format**: Provides ACID transactions, schema evolution, time travel 
- **Direct Lake for serving**: Eliminates import latency while maintaining query performance 

---

## 3. Fabric Fundamentals Required for This Guide

### 3.1 Core Fabric Components

| Component | Purpose | When to Use |
|-----------|---------|-------------|
| **OneLake** | Single logical data lake for the entire organization | Foundation for all data storage; automatically provisioned per tenant |
| **Lakehouse** | Combines data lake storage with SQL analytics endpoint | Bronze/Silver/Gold layers; Spark workloads |
| **Warehouse** | Fully managed T-SQL analytics with ACID | When strict T-SQL compliance required; existing SQL Server workloads |
| **Dataflows Gen2** | Low-code ETL with Power Query | Ingestion, simple transformations, citizen integrators |
| **Pipelines** | Orchestration engine | Workflow coordination, dependency management |
| **Notebooks** | Spark-based code execution | Complex transformations, Python/Scala/R workloads |
| **Semantic Models** | Power BI datasets | Business logic, measures, RLS |

### 3.2 Key Concepts

**Delta Lake**: The default storage format in Fabric Lakehouses . Benefits include:
- ACID transactions for reliability
- Time travel for point-in-time queries
- Schema enforcement and evolution
- Optimized file layout with Z-ordering

**SQL Analytics Endpoint**: Each Lakehouse automatically exposes a T-SQL endpoint, enabling:
- Familiar SQL queries without moving data
- Integration with existing BI tools
- Support for views, stored procedures, and functions

**Direct Lake Mode**: Power BI queries Delta files directly without import 
- No data movement or duplication
- Near real-time data access
- Petabyte-scale analytics
- Combined benefits of Import (performance) and DirectQuery (freshness)

---

## 4. Workspace, Environment & Naming Conventions

### 4.1 Workspace Per Layer Strategy

Microsoft recommends creating separate Lakehouses for each Medallion layer . Extend this to workspace-level isolation:

```
┌─────────────────────────────────────────────────────────────────┐
│                      FABRIC CAPACITY                            │
│  (F64 / F256 / Reserved Instance)                               │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  bronze-workspace│   │ silver-workspace│   │ gold-workspace  │
│  ───────────────│   │ ───────────────│   │ ───────────────│
│  Lakehouse:     │   │ Lakehouse:     │   │ Lakehouse:     │
│  bronze_lh      │   │ silver_lh      │   │ gold_lh        │
│                 │   │                │   │ Warehouse:     │
│  Bronze tables  │   │ Silver tables  │   │ gold_wh (opt)  │
│  (raw)          │   │ (cleansed)     │   │               │
│                 │   │                │   │ Star schemas  │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

### 4.2 Environment Strategy

| Environment | Workspace Naming | Purpose | Access |
|-------------|------------------|---------|--------|
| **Development** | `dev-{project}-{team}` | Individual/collaborative development | Data Engineers, Developers |
| **Test** | `tst-{project}-{team}` | Integration testing, UAT | QA Team, Power Users |
| **Production** | `prd-{project}-{team}` | Live business consumption | End Users (read-only) |

### 4.3 Naming Conventions

**Workspaces:**
```
{env}-{domain}-{layer}-{region}
Example: prd-sales-gold-weu
```

**Lakehouses:**
```
{layer}_lh_{domain}
Example: gold_lh_sales
```

**Tables:**
```
{layer}_{entity}_{type}
- bronze_sales_orders (raw)
- silver_dim_customer (dimension)
- silver_fct_orders (fact - silver level)
- gold_fact_sales (fact - gold level)
- gold_agg_daily_sales (aggregate)
```

**Columns:**
- Snake_case: `customer_id`, `order_date`
- Surrogate keys: `customer_key`, `product_key`
- Audit columns: `_ingestion_ts`, `_source_system`, `_batch_id`

### 4.4 Pre-Deployment Checklist

- [ ] Capacity provisioned (F64 minimum for production)
- [ ] Workspace structure created (dev/tst/prd per layer)
- [ ] Azure AD groups created for RBAC
- [ ] Naming conventions documented and shared
- [ ] Git repository initialized for CI/CD

---

## 5. Ingestion Layer

### 5.1 Source Integration Options

| Source Type | Recommended Method | Frequency | Notes |
|-------------|-------------------|-----------|-------|
| **SQL Server / Oracle** | Dataflows Gen2 + Copy Activity | Incremental (CDC) | Use query for incremental loads |
| **Salesforce / Workday** | Dataflows Gen2 (connectors) | Scheduled (daily) | Handle API rate limits |
| **Azure Blob / ADLS** | Shortcuts + Pipelines | On file arrival | No data movement if using shortcuts |
| **Kafka / Event Hubs** | Eventstream | Real-time | Landing to Bronze Lakehouse |
| **SFTP / Files** | Pipelines (Copy) | Scheduled | Use wildcard paths |

### 5.2 Step-by-Step: Dataflow Gen2 for Incremental Load

**Objective:** Ingest SQL Server sales orders incrementally using LastModifiedDate

1. **Create Dataflow:**
   - Navigate to bronze-workspace
   - New → Dataflow Gen2
   - Name: `ingest_sales_orders`

2. **Configure Source:**
   - Choose SQL Server connector
   - Server: `prod-sql-01.database.windows.net`
   - Database: `sales_db`
   - Query:
     ```sql
     SELECT 
         *,
         'SQL_PROD' AS _source_system,
         GETDATE() AS _ingestion_ts
     FROM sales.orders 
     WHERE LastModifiedDate >= '@{formatDateTime(pipeline().parameters.WatermarkValue, 'yyyy-MM-dd HH:mm:ss')}'
     ```

3. **Add Watermark Parameter:**
   - Parameters → New
   - Name: `WatermarkValue`
   - Type: String
   - Default: `1900-01-01`

4. **Set Destination:**
   - Destination: Lakehouse (bronze_lh)
   - Table: `bronze_sales_orders`
   - Write behavior: Append
   - Enable staging for Delta conversion

5. **Schedule:**
   - Hourly via Pipeline with Lookup activity for watermark

### 5.3 Eventstream for Real-Time Ingestion

**Step-by-Step: Real-time clickstream to Bronze**

1. **Create Eventstream:**
   - New → Eventstream
   - Name: `clickstream_live`

2. **Add Source:**
   - Source: Azure Event Hubs
   - Connection: `clickstream-eh.servicebus.windows.net`

3. **Add Destination:**
   - Destination: Lakehouse (bronze_lh)
   - Table: `bronze_clickstream_raw`
   - Partition by: `ingestion_time` (dynamic)

4. **Configure Transformation (Optional):**
   - Add event processor for cleansing before landing

### 5.4 Partition Strategy

**Bronze Partition Pattern:**
```
Tables/
└── bronze_sales_orders/
    ├── year=2026/
    │   ├── month=03/
    │   │   ├── day=10/
    │   │   │   └── part-00001.snappy.parquet
    │   │   └── day=11/
    │   └── month=04/
    └── year=2027/
```

**Implementation in Dataflow:**
```json
{
  "partitionConfiguration": {
    "partitionColumns": ["year", "month", "day"],
    "partitionBy": "folder"
  }
}
```

### 5.5 Metadata Enrichment

Always add these system columns at ingestion:

```python
# PySpark in Notebook
from pyspark.sql.functions import current_timestamp, lit, input_file_name

df = spark.read.format("parquet").load("path/to/source")
df_enriched = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source_system", lit("SFTP")) \
                .withColumn("_source_file", input_file_name()) \
                .withColumn("_batch_id", lit(spark.sql("SELECT max(batch_id) FROM bronze_audit").collect()[0][0] + 1))

df_enriched.write.format("delta") \
    .partitionBy("year", "month", "day") \
    .mode("append") \
    .save("Tables/bronze_sales_orders")
```

### 5.6 Ingestion Troubleshooting

| Error | Common Cause | Resolution |
|-------|--------------|------------|
| **Delta table schema mismatch** | Source added/dropped columns | Enable schema evolution in Dataflow |
| **Dataflow timeout** | Large data volume | Use staging; split into batches |
| **Eventstream lagging** | High throughput | Increase partitions in Event Hubs |
| **CDC duplicates** | No watermark tracking | Implement idempotent writes with merge |

---

## 6. Bronze Layer (Raw)

### 6.1 Purpose & Characteristics

The Bronze layer is your immutable system of record . It must preserve data exactly as received with full auditability.

**Key Requirements:**
- **Immutable:** Append-only, never update or delete
- **Auditable:** Full source fidelity, including all columns
- **Partitioned:** Time-based for efficient pruning
- **Metadata-rich:** Source tracking, timestamps, batch IDs

### 6.2 Bronze Table Creation

```sql
-- Via SQL Analytics Endpoint
CREATE TABLE bronze_sales_orders
(
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    order_amount DECIMAL(18,2),
    status STRING,
    source_system STRING,
    -- Metadata columns
    _source_system STRING,
    _ingestion_timestamp TIMESTAMP,
    _batch_id LONG,
    _source_file STRING
)
USING DELTA
PARTITIONED BY (year(order_date), month(order_date), day(order_date))
TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.appendOnly' = 'true'
)
LOCATION 'abfss://bronze-workspace@onelake.dfs.fabric.microsoft.com/bronze_lh/Tables/bronze_sales_orders';
```

### 6.3 Bronze Table Checklist

- [ ] Partitioned by date (year/month/day)
- [ ] All source columns preserved
- [ ] Metadata columns added (`_source_system`, `_ingestion_ts`, `_batch_id`)
- [ ] Delta format with append-only property
- [ ] Data retention policy defined (e.g., 90 days raw, then cold storage)

### 6.4 Bronze Data Quality Validation

Implement basic validation to detect source issues:

```python
# Bronze validation notebook
from pyspark.sql.functions import count, isnan, when, col

df = spark.table("bronze_lh.bronze_sales_orders")

# Basic quality metrics
quality_metrics = df.agg(
    count("*").alias("total_rows"),
    count("customer_id").alias("non_null_customer_id"),
    (count("*") - count("customer_id")).alias("null_customer_id_count")
).collect()

# Log to monitoring table
spark.sql(f"""
    INSERT INTO bronze_quality_log 
    VALUES (
        '{date.today()}',
        'bronze_sales_orders',
        {quality_metrics[0]['total_rows']},
        {quality_metrics[0]['null_customer_id_count']}
    )
""")
```

---

## 7. Silver Layer (Cleansed & Conformed)

### 7.1 Purpose & Characteristics

The Silver layer transforms raw Bronze data into trusted, enterprise-wide data assets . This is where data becomes analytics-ready.

**Key Transformations:**
- Deduplication
- Data type standardization
- NULL handling with business rules
- Reference data enrichment
- Slowly Changing Dimensions (SCD Type 2)

### 7.2 Silver Layer Patterns

#### 7.2.1 Fact Table Transformation (PySpark)

```python
# silver_sales_orders notebook
from pyspark.sql.functions import col, to_date, row_number, when, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Read from Bronze
bronze_df = spark.table("bronze_lh.bronze_sales_orders")

# Deduplicate (keep latest by LastModifiedDate)
window_spec = Window.partitionBy("order_id").orderBy(col("LastModifiedDate").desc())
deduped_df = bronze_df.withColumn("rn", row_number().over(window_spec)) \
                      .filter(col("rn") == 1) \
                      .drop("rn")

# Clean and standardize
cleaned_df = deduped_df.select(
    col("order_id"),
    col("customer_id").cast("string"),
    to_date(col("order_date")).alias("order_date"),
    col("order_amount").cast("decimal(18,2)"),
    when(col("status").isin("COMPLETED", "SHIPPED"), "COMPLETED")
    .otherwise("PENDING").alias("order_status"),
    col("_source_system"),
    col("_ingestion_timestamp")
)

# Join reference data (example: currency rates)
ref_rates = spark.table("reference_lh.currency_rates")
enriched_df = cleaned_df.join(ref_rates, 
                              (cleaned_df.order_date >= ref_rates.effective_date) & 
                              (cleaned_df.order_date < ref_rates.end_date), 
                              "left")

# Write to Silver with merge (upsert)
silver_table = DeltaTable.forPath(spark, "Tables/silver_sales_orders")

silver_table.alias("target").merge(
    enriched_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

#### 7.2.2 SCD Type 2 Dimension (SQL)

```sql
-- Create silver dimension with SCD Type 2
CREATE OR REPLACE TABLE silver_dim_customer
(
    customer_key BIGINT GENERATED BY DEFAULT AS IDENTITY,
    customer_id STRING,
    customer_name STRING,
    customer_email STRING,
    customer_region STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    dbt_scd_id STRING,
    _ingested_at TIMESTAMP
)
USING DELTA;

-- Merge for SCD2 (via Spark or T-SQL MERGE)
MERGE INTO silver_dim_customer AS target
USING (
    SELECT 
        customer_id,
        customer_name,
        customer_email,
        customer_region,
        current_date() AS effective_date,
        '9999-12-31' AS end_date,
        true AS is_current,
        hash(customer_name, customer_email, customer_region) AS dbt_scd_id
    FROM bronze_customer_updates
) AS source
ON target.customer_id = source.customer_id AND target.is_current = true
WHEN MATCHED AND target.dbt_scd_id != source.dbt_scd_id THEN
    UPDATE SET target.is_current = false, target.end_date = current_date()
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, customer_email, customer_region, 
            effective_date, end_date, is_current, dbt_scd_id)
    VALUES (source.customer_id, source.customer_name, source.customer_email,
            source.customer_region, source.effective_date, source.end_date,
            source.is_current, source.dbt_scd_id);
```

### 7.3 Silver Data Quality Framework

| Check Type | Implementation | Action on Failure |
|------------|----------------|-------------------|
| **Not Null** | `customer_id IS NOT NULL` | Reject row, alert |
| **Referential** | `product_id IN (SELECT product_id FROM dim_product)` | Log, use dummy key |
| **Range** | `order_amount > 0` | Flag, notify business |
| **Uniqueness** | `order_line_id UNIQUE` | Deduplicate, keep latest |
| **Freshness** | Max(order_date) > yesterday | Alert if stale |

### 7.4 Silver Layer Checklist

- [ ] Deduplication logic implemented per business key
- [ ] Data types standardized (dates as DATE, decimals as DECIMAL)
- [ ] NULL handling strategy documented and applied
- [ ] Reference data joined where applicable
- [ ] SCD Type 2 implemented for core dimensions
- [ ] Audit columns preserved (source tracking)
- [ ] Data quality tests automated
- [ ] Table statistics collected for optimization

---

## 8. Gold Layer (Business-Ready)

### 8.1 Purpose & Characteristics

The Gold layer contains curated, business-defined datasets optimized for consumption . This is where we implement star schemas, aggregates, and business logic.

**Key Characteristics:**
- Star schema design (facts and dimensions)
- Business-friendly column names
- Pre-calculated measures
- Aggregated tables for performance
- Row-level security ready

### 8.2 Star Schema Design

#### 8.2.1 Dimension Tables

```sql
-- Gold dimension (conformed, SCD2 already resolved)
CREATE OR REPLACE TABLE gold_dim_customer
USING DELTA
AS SELECT
    customer_key,
    customer_id,
    customer_name,
    customer_email,
    customer_region,
    customer_segment,
    acquisition_date
FROM silver_dim_customer
WHERE is_current = true;

-- Date dimension (enterprise standard)
CREATE OR REPLACE TABLE gold_dim_date
USING DELTA
AS SELECT
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_week,
    is_weekend,
    is_holiday
FROM enterprise_date_dim;
```

#### 8.2.2 Fact Table

```sql
-- Gold fact table (star schema)
CREATE OR REPLACE TABLE gold_fact_sales
USING DELTA
AS SELECT
    -- Surrogate keys
    ROW_NUMBER() OVER (ORDER BY so.order_id, so.line_id) AS sales_key,
    d.date_key AS order_date_key,
    c.customer_key,
    p.product_key,
    e.employee_key,
    
    -- Business keys (for reference)
    so.order_id,
    so.order_line_id,
    
    -- Measures
    so.quantity,
    so.unit_price,
    so.discount,
    so.quantity * so.unit_price AS gross_amount,
    so.quantity * so.unit_price * (1 - so.discount) AS net_amount,
    so.tax_amount,
    
    -- Calculated measures
    so.quantity * so.unit_price * (1 - so.discount) - so.cost_amount AS profit,
    
    -- Business attributes
    so.order_status,
    so.shipment_status,
    
    -- Metadata
    current_timestamp() AS _created_at
FROM silver_sales_orders so
JOIN gold_dim_customer c ON so.customer_id = c.customer_id
JOIN gold_dim_product p ON so.product_id = p.product_id
JOIN gold_dim_date d ON CAST(so.order_date AS DATE) = d.full_date
LEFT JOIN gold_dim_employee e ON so.salesperson_id = e.employee_id
WHERE so.order_status IN ('COMPLETED', 'SHIPPED');
```

### 8.3 Aggregated Tables for Performance

For large fact tables (billions of rows), create aggregates:

```sql
-- Daily sales aggregates
CREATE OR REPLACE TABLE gold_agg_daily_sales
USING DELTA
AS SELECT
    d.date_key,
    d.year,
    d.month,
    d.day,
    c.customer_region,
    p.product_category,
    COUNT(DISTINCT so.order_id) AS order_count,
    SUM(so.quantity) AS total_quantity,
    SUM(so.net_amount) AS total_sales,
    SUM(so.profit) AS total_profit
FROM gold_fact_sales so
JOIN gold_dim_date d ON so.order_date_key = d.date_key
JOIN gold_dim_customer c ON so.customer_key = c.customer_key
JOIN gold_dim_product p ON so.product_key = p.product_key
GROUP BY d.date_key, d.year, d.month, d.day, 
         c.customer_region, p.product_category;

-- Optimize with Z-ordering
OPTIMIZE gold_agg_daily_sales
ZORDER BY (date_key, customer_region, product_category);
```

### 8.4 Gold Layer Optimization

| Technique | When to Use | Implementation |
|-----------|-------------|----------------|
| **Z-ordering** | High-cardinality columns in large tables | `OPTIMIZE table ZORDER BY (col1, col2)` |
| **Liquid clustering** | Tables with frequent updates | Delta Lake 3.0+ with `CLUSTER BY` |
| **Partitioning** | Date-based queries | `PARTITIONED BY (year, month)` |
| **Bloom filters** | Join-heavy workloads | `ALTER TABLE ADD BLOOM INDEX` |
| **Materialized views** | Pre-computed aggregates | `CREATE MATERIALIZED VIEW ...` |

### 8.5 Gold Layer Checklist

- [ ] Star schema implemented (facts + dimensions)
- [ ] Surrogate keys used for joins
- [ ] Business-friendly column names
- [ ] Aggregated tables created for common queries
- [ ] Table statistics collected
- [ ] Optimized with Z-ordering/clustering
- [ ] Row-level security columns added if needed
- [ ] Documentation updated with business definitions

---

## 9. Warehouse Design & Semantic Model

### 9.1 Lakehouse vs. Warehouse Decision Matrix

| Requirement | Lakehouse | Warehouse | Recommendation |
|-------------|-----------|-----------|----------------|
| **Data size** | Petabytes | TBs scale | Lakehouse for >10TB |
| **SQL workloads** | T-SQL endpoint | Full T-SQL | Warehouse for complex stored procs |
| **Mixed workloads** | Spark + SQL | SQL only | Lakehouse for flexibility |
| **Existing SSAS** | Semantic model | DirectQuery | Warehouse for migration |
| **Time travel** | Delta support | Limited | Lakehouse for audit needs |

**Our Recommendation:** Use Lakehouse for Gold layer with SQL Analytics endpoint. Only use Warehouse when you need strict T-SQL features not available in Lakehouse.

### 9.2 Creating a Semantic Model

The semantic model (Power BI dataset) can be created directly on the Gold Lakehouse:

**Option A: Default Semantic Model**
1. Navigate to gold_lh in gold-workspace
2. Click "New Semantic Model" 
3. Select tables (fact_sales, dim_customer, dim_product, dim_date)
4. Auto-detected relationships will appear

**Option B: Manual Model Definition**
```json
{
  "name": "Sales Analytics Model",
  "tables": [
    {
      "name": "Sales",
      "source": {
        "type": "lakehouse",
        "table": "gold_fact_sales"
      }
    },
    {
      "name": "Customer",
      "source": {
        "type": "lakehouse",
        "table": "gold_dim_customer"
      }
    }
  ],
  "relationships": [
    {
      "fromTable": "Sales",
      "fromColumn": "customer_key",
      "toTable": "Customer",
      "toColumn": "customer_key",
      "cardinality": "many_to_one"
    }
  ]
}
```

### 9.3 Measures Definition

```dax
-- In the semantic model
Total Sales = SUM('Sales'[net_amount])

Total Profit = SUM('Sales'[profit])

Profit Margin = DIVIDE([Total Profit], [Total Sales], 0)

Sales YoY Growth = 
VAR CurrentYearSales = [Total Sales]
VAR PreviousYearSales = CALCULATE(
    [Total Sales],
    SAMEPERIODLASTYEAR('Date'[full_date])
)
RETURN DIVIDE(CurrentYearSales - PreviousYearSales, PreviousYearSales, 0)

Top Customers = 
TOPN(10, VALUES('Customer'[customer_name]), [Total Sales])
```

### 9.4 Performance Optimization for Semantic Models

**Aggregation Tables Pattern:**

```xml
<!-- In Power BI Desktop -->
<Aggregations>
    <Aggregation Table="Sales">
        <DimensionUsage Name="Customer" />
        <DimensionUsage Name="Product" />
        <DimensionUsage Name="Date" />
        <Measure Name="Total Sales" Aggregation="Sum(Amount)" />
        <Granularity>Customer, Product, Date</Granularity>
    </Aggregation>
    <Mapping>
        <Map Source="fact_sales" Target="Sales_Agg" 
             Condition="Date >= Date" />
    </Mapping>
</Aggregations>
```

### 9.5 Warehouse/Semantic Checklist

- [ ] Decision made: Lakehouse vs. Warehouse
- [ ] SQL Analytics endpoint tested with key queries
- [ ] Semantic model tables selected
- [ ] Relationships defined and validated
- [ ] Key measures created and tested
- [ ] Aggregations defined for large tables
- [ ] Model published to service

---

## 10. Power BI Consumption

### 10.1 Connection Modes Comparison

| Mode | Performance | Freshness | Data Size | Best For |
|------|-------------|-----------|-----------|----------|
| **Direct Lake** | Fast | Near real-time | Unlimited | Large datasets (recommended) |
| **Import** | Fastest | Scheduled | <1GB | Small, complex models |
| **DirectQuery** | Variable | Real-time | Unlimited | Operational reports |

**Direct Lake** is Microsoft's breakthrough mode for Fabric . It combines:
- No data movement (query Delta files directly)
- In-memory performance via VertiPaq engine
- Unlimited data size (petabytes)
- Automatic refresh (no scheduled imports)

### 10.2 Step-by-Step: Connect Power BI to Gold Lakehouse (Direct Lake)

1. **From Power BI Desktop:**
   - Get Data → Power BI Semantic Models (or OneLake)
   - Select your workspace → Semantic Model
   - Or: DirectLake to Warehouse via SQL endpoint

2. **From Fabric Portal:**
   - Navigate to gold_lh
   - Copy SQL Analytics endpoint connection string
   - In Power BI Desktop: Get Data → Azure → Azure SQL Database
   - Paste endpoint (Server: `gold-workspace.database.windows.net`)

3. **Verify Direct Lake mode:**
   - In Power BI Desktop, check model view
   - Table storage mode should show "DirectLake"

### 10.3 Building Reports on Large Datasets

**Best Practices:**

1. **Start with aggregates**: Use pre-aggregated tables (`gold_agg_daily_sales`) for high-level dashboards
2. **Drill-through to detail**: Configure drill-through actions to fact tables for detailed views
3. **Field parameters**: Use parameters to let users switch between measures
4. **Calculated tables**: Push calculations to Gold layer when possible

**Example Report Structure:**
```
Executive Dashboard (Direct Lake on Aggregates)
├── Summary KPIs (cards)
├── Monthly Trends (line chart)
└── Regional Performance (map)

Operational Detail Page (Direct Lake on Facts)
├── Filters (Date, Region, Product)
└── Transaction List (table)
```

### 10.4 Row-Level Security (RLS) in Semantic Model

```dax
-- In Power BI Desktop, Manage Roles
Role: 'Regional Manager'
Table: Customer
Filter: [region] = USERPRINCIPALNAME()

-- Or using a security table
Role: 'Sales Person'
Table: Sales
Filter: [salesperson_email] = USERPRINCIPALNAME()
```

**Dynamic RLS with Security Table:**
```dax
[region] IN 
    SELECT allowed_region 
    FROM gold_rls_user_region 
    WHERE user_principal_name = USERPRINCIPALNAME()
```

### 10.5 Power BI Consumption Checklist

- [ ] Connection mode selected (Direct Lake recommended)
- [ ] Semantic model published to service
- [ ] RLS roles defined and tested
- [ ] Gateway configured (if using on-prem data)
- [ ] Scheduled refresh (only if using Import mode)
- [ ] Dashboard performance tested with representative users
- [ ] Mobile layout optimized

---

## 11. Security & Governance

### 11.1 OneLake Access Control Hierarchy

```
┌─────────────────────────────────────────────────┐
│                AZURE ACTIVE DIRECTORY           │
│           (Authentication & Groups)              │
└─────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│              FABRIC CAPACITY ADMIN               │
│  • Capacity admins                               │
│  • Tenant settings                               │
└─────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│               WORKSPACE ROLES                     │
├───────────────┬─────────────────────────────────┤
│  Admin        │ Full control                     │
│  Member       │ Publish, modify items             │
│  Contributor  │ View, refresh, execute            │
│  Viewer       │ Read-only access                  │
└───────────────┴─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│             DATA ITEM PERMISSIONS                 │
│  • Lakehouse read/write                          │
│  • SQL endpoint access                           │
│  • Semantic model view                           │
│  • Row/column level security (RLS/CLS)           │
└─────────────────────────────────────────────────┘
```

### 11.2 Recommended RBAC by User Type

| User Type | Bronze WS | Silver WS | Gold WS | Semantic Model |
|-----------|-----------|-----------|---------|----------------|
| **Data Engineer** | Admin | Admin | Admin | Read |
| **Data Analyst** | Viewer | Contributor | Contributor | Read |
| **BI Developer** | None | Viewer | Contributor | Contribute |
| **Report Consumer** | None | None | Viewer | Read (RLS) |
| **Auditor** | Viewer | Viewer | Viewer | None |

### 11.3 Implementing RLS at Gold Layer

**Option 1: Semantic Model RLS (Recommended)**
- Define roles in Power BI Desktop
- Publish and assign users via workspace

**Option 2: Database-level RLS in Warehouse**
```sql
-- Create security policy in Gold Warehouse
CREATE SCHEMA Security;
GO

CREATE FUNCTION Security.fn_securitypredicate(@Region AS sysname)  
    RETURNS TABLE  
WITH SCHEMABINDING  
AS  
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @Region = USER_NAME() OR USER_NAME() = 'DataSteward';
GO

CREATE SECURITY POLICY RegionFilter  
ADD FILTER PREDICATE Security.fn_securitypredicate(Region)   
ON dbo.Customer  
WITH (STATE = ON);
```

### 11.4 Data Masking for PII

```sql
-- In Gold Warehouse
ALTER TABLE gold_dim_customer
ALTER COLUMN customer_email
ADD MASKED WITH (FUNCTION = 'email()');

ALTER TABLE gold_dim_customer
ALTER COLUMN phone_number
ADD MASKED WITH (FUNCTION = 'partial(0, "XXX-XXX-", 4)');
```

### 11.5 Microsoft Purview Integration

1. **Register Fabric tenant** in Purview
2. **Scan workspaces** for automated data classification
3. **Apply sensitivity labels** via Purview policies
4. **Enable lineage** tracking across pipelines

### 11.6 Security Audit Checklist

- [ ] Azure AD groups created for each role
- [ ] Workspace roles assigned (no individual user assignments)
- [ ] RLS implemented in semantic model
- [ ] PII columns masked
- [ ] Audit logging enabled
- [ ] External sharing restricted
- [ ] Data retention policies documented

---

## 12. CI/CD & Automation

### 12.1 Deployment Pipeline Architecture

Microsoft Fabric provides native Deployment Pipelines for promoting content across environments .

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Development   │────▶│      Test       │────▶│   Production    │
│   Workspace     │     │   Workspace     │     │   Workspace     │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ • Bronze Lake   │     │ • Bronze Lake   │     │ • Bronze Lake   │
│ • Silver Lake   │     │ • Silver Lake   │     │ • Silver Lake   │
│ • Gold Lake     │     │ • Gold Lake     │     │ • Gold Lake     │
│ • Pipelines     │     │ • Pipelines     │     │ • Pipelines     │
│ • Notebooks     │     │ • Notebooks     │     │ • Notebooks     │
│ • Semantic Mdl  │     │ • Semantic Mdl  │     │ • Semantic Mdl  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
    Git commit              Automated               Approval
    feature branch          deployment               gate
        │                  & testing                  │
        ▼                       ▼                      ▼
    ┌─────────────────────────────────────────────────────┐
    │               GIT REPOSITORY (Azure DevOps)         │
    │  • main branch (prod)                               │
    │  • develop branch (test)                            │
    │  • feature/* (dev)                                  │
    └─────────────────────────────────────────────────────┘
```

### 12.2 Setting Up Deployment Pipelines

**Step-by-Step:**

1. **Create Pipeline:**
   - Navigate to Development workspace
   - New → Deployment Pipeline
   - Name: `Sales Analytics CI/CD`
   - Assign Development workspace

2. **Add Stages:**
   ```
   Stage 1: Development ← current
   Stage 2: Test
   Stage 3: Production
   ```

3. **Configure Rules (Parameterization):**
   ```json
   {
     "rules": [
       {
         "type": "DataPipeline",
         "parameters": {
           "SourceServer": {
             "value": "dev-sql-01.database.windows.net"
           },
           "TargetServer": {
             "value": "prd-sql-01.database.windows.net"
           }
         }
       },
       {
         "type": "Lakehouse",
         "connectionOverrides": {
           "bronze_lh": "bronze_lh_prd"
         }
       }
     ]
   }
   ```

4. **Deploy:**
   - In Test stage: "Deploy from Development"
   - In Production: "Deploy from Test"

### 12.3 Git Integration

**Connecting to Azure DevOps:**

```bash
# PowerShell example for Git operations
$workspaceId = "your-workspace-id"
$gitRepoUrl = "https://dev.azure.com/org/project/_git/fabric-repo"

# Connect workspace to Git branch
Connect-FabricWorkspaceToGit `
    -WorkspaceId $workspaceId `
    -GitProvider "AzureDevOps" `
    -RepositoryUrl $gitRepoUrl `
    -BranchName "develop"
```

**Branch Strategy:**
- `main`: Production-ready code
- `develop`: Test/integration branch
- `feature/*`: Development branches
- `release/*`: Release preparation

### 12.4 Automated Testing with Notebooks

```python
# test_silver_layer notebook
import unittest
from pyspark.sql import SparkSession

class TestSilverLayer(unittest.TestCase):
    
    def test_no_duplicates(self):
        df = spark.table("silver_lh.silver_sales_orders")
        duplicate_count = df.groupBy("order_id").count().filter("count > 1").count()
        self.assertEqual(duplicate_count, 0, f"Found {duplicate_count} duplicate order_ids")
    
    def test_referential_integrity(self):
        sales = spark.table("silver_lh.silver_sales_orders")
        customers = spark.table("silver_lh.dim_customer")
        
        orphaned = sales.join(customers, "customer_id", "left_anti").count()
        self.assertEqual(orphaned, 0, f"Found {orphaned} orphaned sales records")
    
    def test_date_range(self):
        df = spark.table("silver_lh.silver_sales_orders")
        max_date = df.agg({"order_date": "max"}).collect()[0][0]
        min_date = df.agg({"order_date": "min"}).collect()[0][0]
        
        # Ensure data is recent (last 30 days)
        from datetime import datetime, timedelta
        self.assertGreaterEqual(max_date, datetime.now() - timedelta(days=30))

# Run tests
if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)
```

### 12.5 CI/CD Checklist

- [ ] Deployment pipeline created with dev/test/prod stages
- [ ] Parameterization rules configured
- [ ] Git repository connected
- [ ] Branch policies defined
- [ ] Automated tests implemented
- [ ] Approval gates configured for production
- [ ] Rollback procedures documented

---

## 13. Monitoring, Observability & Cost Control

### 13.1 Key Metrics to Monitor

| Category | Metric | Warning Threshold | Critical Threshold |
|----------|--------|-------------------|-------------------|
| **Capacity** | CU % utilization | >70% sustained | >90% sustained |
| **Capacity** | Throttling events | >0 in 1 hour | >10 in 1 hour |
| **Data Freshness** | Max load timestamp | >24h stale | >48h stale |
| **Pipeline** | Failure rate | >5% of runs | >20% of runs |
| **Data Quality** | Null/Duplicate % | >5% | >10% |
| **Semantic Model** | Query duration | >5 sec avg | >10 sec avg |

### 13.2 Setting Up Monitoring

**Option 1: Fabric Capacity Metrics App**
1. Install from AppSource: "Microsoft Fabric Capacity Metrics"
2. Connect to your capacity
3. View dashboards for CU utilization, throttling

**Option 2: Custom Monitoring Lakehouse**

```sql
-- Create monitoring tables
CREATE TABLE monitoring.pipeline_runs (
    run_id STRING,
    pipeline_name STRING,
    layer STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    rows_processed LONG,
    error_message STRING,
    duration_seconds LONG
);

CREATE TABLE monitoring.data_quality_log (
    check_date DATE,
    table_name STRING,
    check_name STRING,
    status STRING,
    failed_count LONG,
    total_count LONG
);
```

**Python Logging in Notebooks:**
```python
from datetime import datetime
import logging

class FabricLogger:
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.start_time = datetime.now()
        
    def log_pipeline_start(self):
        spark.sql(f"""
            INSERT INTO monitoring.pipeline_runs 
            (run_id, pipeline_name, start_time, status)
            VALUES (
                '{self.run_id}',
                '{self.pipeline_name}',
                '{self.start_time}',
                'RUNNING'
            )
        """)
    
    def log_pipeline_end(self, status, rows=0, error=None):
        duration = (datetime.now() - self.start_time).seconds
        spark.sql(f"""
            UPDATE monitoring.pipeline_runs
            SET end_time = '{datetime.now()}',
                status = '{status}',
                rows_processed = {rows},
                error_message = '{error}',
                duration_seconds = {duration}
            WHERE run_id = '{self.run_id}'
        """)
```

### 13.3 Alerting Runbook

**Alert Configuration (via Azure Monitor):**

```json
{
  "alertRule": {
    "name": "Pipeline Failure Alert",
    "condition": {
      "query": "monitoring_pipeline_runs | where status == 'FAILED' | where start_time > ago(15m) | count",
      "threshold": "> 0"
    },
    "actions": [
      {
        "actionGroup": "DataEngineeringTeam",
        "emailSubject": "Pipeline Failure Detected",
        "webhook": "https://.../teams-webhook"
      }
    ]
  }
}
```

**Email Alert Template:**
```
Subject: [Fabric Alert] Pipeline Failure: {pipeline_name}

Pipeline: {pipeline_name}
Layer: {layer}
Run ID: {run_id}
Failed at: {failure_time}
Error: {error_message}

Next Steps:
1. Check logs in monitoring.pipeline_runs
2. Review data quality metrics
3. Restart from failed step

Runbook: https://wiki.company/fabric-runbooks/pipeline-failure
```

### 13.4 Cost Optimization Strategies

Based on enterprise patterns, here are proven cost-saving tactics :

#### 13.4.1 BI Dashboard Optimization

**Semantic Model Aggregations:**
```dax
-- Pre-aggregate monthly sales
DEFINE TABLE Sales_Monthly_Agg = 
SUMMARIZECOLUMNS(
    'Date'[Year],
    'Date'[Month],
    'Customer'[Region],
    "TotalSales", SUM('Sales'[net_amount]),
    "OrderCount", COUNTROWS('Sales')
)
```

**Impact:** Reduces Premium capacity utilization by up to 40% .

#### 13.4.2 Incremental Refresh

```json
{
  "refreshPolicy": {
    "type": "incremental",
    "incrementalGranularity": "Day",
    "incrementalPeriods": 7,
    "rollingWindowGranularity": "Year",
    "rollingWindowPeriods": 5,
    "detectDataChanges": true
  }
}
```

**Impact:** Refresh costs reduced by 70% for large fact tables .

#### 13.4.3 Workspace Auto-Pause

```powershell
# PowerShell script to auto-pause dev workspaces
$idleThresholdHours = 2
$workspaces = @("dev-sales-bronze", "dev-sales-silver", "dev-sales-gold")

foreach ($ws in $workspaces) {
    $usage = Get-FabricWorkspaceUsage -WorkspaceId $ws -Days 1
    if ($usage.LastActivity -lt (Get-Date).AddHours(-$idleThresholdHours)) {
        Suspend-FabricWorkspace -WorkspaceId $ws
        Send-EmailAlert -Subject "Workspace auto-paused: $ws"
    }
}
```

**Impact:** 5-6x cost reduction for dev/test environments .

#### 13.4.4 Query Optimization

```sql
-- Instead of SELECT *
SELECT 
    customer_key,
    SUM(order_amount) as total_sales
FROM gold_fact_sales
WHERE order_date >= '2026-01-01'
GROUP BY customer_key

-- Use partition pruning
WHERE order_date >= '2026-01-01'  -- Uses partition filter
```

### 13.5 Cost Control Checklist

- [ ] Capacity metrics dashboard configured
- [ ] Alerts set for CU > 70%
- [ ] Incremental refresh implemented for large tables
- [ ] Dev workspaces scheduled to auto-pause
- [ ] Query patterns optimized (no SELECT *)
- [ ] Aggregation tables created for common queries
- [ ] Data retention policies enforced
- [ ] Monthly cost review scheduled

---

## 14. Operational Runbooks & Playbooks

### 14.1 Onboarding New Data Source

**Runbook ID:** OPS-101
**Owner:** Data Engineering Team
**Estimated Time:** 2-4 hours

**Steps:**

1. **Requirements Gathering**
   - [ ] Identify source system owner
   - [ ] Document data volume (rows/day, growth rate)
   - [ ] Determine update frequency (real-time/daily/hourly)
   - [ ] Identify PII/sensitive columns
   - [ ] Get business approval

2. **Source Assessment**
   - [ ] Test connectivity from Fabric
   - [ ] Validate schema and data types
   - [ ] Check for CDC capabilities (timestamp columns)
   - [ ] Measure initial data volume

3. **Bronze Implementation**
   - [ ] Create bronze ingestion pipeline
   - [ ] Add metadata columns
   - [ ] Configure partitioning strategy
   - [ ] Set up monitoring alerts

   ```python
   # Sample bronze ingestion check
   def validate_bronze_ingestion():
       df = spark.table("bronze_lh.new_source")
       row_count = df.count()
       assert row_count > 0, "No rows ingested"
       assert df.filter(df._ingestion_ts.isNull()).count() == 0, "Missing ingestion timestamp"
   ```

4. **Silver Implementation**
   - [ ] Define deduplication logic
   - [ ] Create conformed dimension mappings
   - [ ] Implement data quality checks
   - [ ] Join reference data

5. **Gold Implementation**
   - [ ] Design star schema integration
   - [ ] Update semantic model
   - [ ] Create measures
   - [ ] Update documentation

6. **Handover**
   - [ ] Update data catalog
   - [ ] Notify stakeholders
   - [ ] Create support documentation

### 14.2 Incident Runbook: Pipeline Failure

**Runbook ID:** INC-201
**Severity:** P1 (Critical) / P2 (High)

**Detection:**
- Alert: "Pipeline Failure Alert" email
- Symptoms: Data not updated, stale reports

**Immediate Actions (First 15 minutes):**

1. **Identify Failure Point**
   ```sql
   -- Check monitoring logs
   SELECT * FROM monitoring.pipeline_runs
   WHERE start_time > now() - interval 1 hour
   AND status = 'FAILED'
   ORDER BY start_time DESC;
   ```

2. **Check Error Message**
   - Review pipeline error details
   - Note activity name and error code

3. **Assess Impact**
   - Which downstream tables affected?
   - Which reports impacted?
   - Notify stakeholders if high impact

**Resolution Steps:**

| Error Pattern | Likely Cause | Resolution |
|---------------|--------------|------------|
| **Source connection timeout** | Source system down | Retry with backoff; contact source owner |
| **Schema mismatch** | Source added columns | Update bronze schema evolution settings |
| **Out of memory** | Data volume spike | Scale up compute; optimize transformations |
| **Duplicate key violation** | Bad dedup logic | Review merge condition; fix and rerun |

**Restart Procedure:**
1. Fix underlying issue
2. Clear partial data (if applicable)
3. Restart from failed step
4. Validate row counts post-run

**Post-Mortem:**
- [ ] Root cause documented
- [ ] Prevention measures identified
- [ ] Runbook updated
- [ ] Stakeholders notified of resolution

### 14.3 Incident Runbook: Data Quality Issue

**Runbook ID:** INC-202
**Severity:** P2 (High)

**Detection:**
- Data quality alert (nulls > threshold)
- Business user reports incorrect numbers

**Investigation:**

```sql
-- Check quality metrics
SELECT * FROM monitoring.data_quality_log
WHERE table_name = 'gold_fact_sales'
ORDER BY check_date DESC
LIMIT 7;

-- Validate source vs target counts
SELECT 
    'bronze' as layer, COUNT(*) as row_count 
FROM bronze_lh.bronze_sales_orders 
WHERE order_date = '2026-03-01'
UNION ALL
SELECT 
    'gold' as layer, COUNT(*) 
FROM gold_lh.gold_fact_sales 
WHERE order_date_key = (SELECT date_key FROM gold_dim_date WHERE full_date = '2026-03-01');
```

**Resolution Steps:**

1. **Isolate the Issue**
   - Identify affected rows/time period
   - Determine if issue is in source or transformation

2. **Fix Bad Data**
   - If source issue: Work with source owner
   - If transformation bug: Fix code and reprocess

3. **Data Correction Options**

   **Option A: Reprocess affected partition**
   ```python
   # Reprocess specific date range
   spark.sql("""
       DELETE FROM gold_fact_sales 
       WHERE order_date_key IN (
           SELECT date_key FROM gold_dim_date 
           WHERE full_date BETWEEN '2026-03-01' AND '2026-03-07'
       )
   """)
   # Rerun pipeline for those dates
   ```

   **Option B: Time travel to correct version**
   ```sql
   -- If issue is recent, rollback to previous version
   RESTORE TABLE gold_fact_sales TO VERSION AS OF 123456;
   ```

4. **Validation**
   ```sql
   -- Validate fix
   SELECT 
       COUNT(*) as total_rows,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customers,
       MIN(order_date_key) as min_date,
       MAX(order_date_key) as max_date
   FROM gold_fact_sales;
   ```

5. **Communicate**
   - Notify stakeholders of fix
   - Update incident ticket

### 14.4 Rollback Procedures

**Scenario:** Bad deployment to production

**Step 1: Assess Rollback Options**

| Option | Time | Data Loss | Best For |
|--------|------|-----------|----------|
| Git revert + redeploy | 15-30 min | None | Code issues only |
| Table version rollback | 5-10 min | Recent changes lost | Data corruption |
| Full workspace restore | 1-2 hours | All recent changes | Catastrophic failure |

**Step 2: Execute Rollback**

**Option A: Git Revert**
```bash
git revert HEAD~1
git push origin main
# Deployment pipeline auto-deploys to prod
```

**Option B: Delta Time Travel**
```sql
-- Find last good version
DESCRIBE HISTORY gold_fact_sales;

-- Restore to that version
RESTORE TABLE gold_fact_sales TO VERSION AS OF 123456;
```

**Option C: Workspace Restore**
1. Navigate to Production workspace
2. Settings → Workspace restore
3. Select restore point (prior to deployment)
4. Confirm restore

**Step 3: Validate**
- Run smoke tests
- Verify row counts
- Check with business users

---

## 15. Templates & Artifacts

### 15.1 Access Control Matrix Template

```csv
Group/User,Workspace,Role,Justification,Expiry
SG_DataEngineers,prd-sales-gold,Admin,"Pipeline management",Never
SG_DataAnalysts,prd-sales-gold,Contributor,"Build reports",Never
SG_Executives,prd-sales-gold,Viewer,"View dashboards",Never
contractor_john,dev-sales-bronze,Contributor,"Project contract",2026-12-31
```

### 15.2 Pipeline Template (JSON)

```json
{
  "name": "bronze_to_silver_sales",
  "activities": [
    {
      "name": "Validate Bronze",
      "type": "Notebook",
      "linkedServiceName": "bronze_workspace",
      "typeProperties": {
        "notebookPath": "/Notebooks/validate_bronze_sales"
      }
    },
    {
      "name": "Transform to Silver",
      "type": "Notebook",
      "dependsOn": [
        {
          "activity": "Validate Bronze",
          "dependencyConditions": ["Succeeded"]
        }
      ],
      "typeProperties": {
        "notebookPath": "/Notebooks/bronze_to_silver_sales",
        "baseParameters": {
          "source_table": "bronze_sales_orders",
          "target_table": "silver_sales_orders",
          "watermark": "@pipeline().parameters.watermark"
        }
      }
    },
    {
      "name": "Run Quality Checks",
      "type": "Notebook",
      "dependsOn": [
        {
          "activity": "Transform to Silver",
          "dependencyConditions": ["Succeeded"]
        }
      ],
      "typeProperties": {
        "notebookPath": "/Notebooks/quality_checks_silver"
      }
    }
  ]
}
```

### 15.3 Semantic Model Template (TMSL)

```json
{
  "createOrReplace": {
    "object": {
      "database": "Sales Analytics Model"
    },
    "model": {
      "tables": [
        {
          "name": "Sales",
          "source": {
            "type": "lakehouse",
            "workspace": "prd-sales-gold",
            "lakehouse": "gold_lh_sales",
            "table": "gold_fact_sales"
          }
        },
        {
          "name": "Customer",
          "source": {
            "type": "lakehouse",
            "workspace": "prd-sales-gold",
            "lakehouse": "gold_lh_sales",
            "table": "gold_dim_customer"
          }
        }
      ],
      "relationships": [
        {
          "fromTable": "Sales",
          "fromColumn": "customer_key",
          "toTable": "Customer",
          "toColumn": "customer_key",
          "crossFilteringBehavior": "oneDirection",
          "securityFilteringBehavior": "oneDirection"
        }
      ],
      "cultures": [
        {
          "name": "en-US",
          "translations": []
        }
      ]
    }
  }
}
```

### 15.4 Monitoring Dashboard Template

```sql
-- Create monitoring views
CREATE VIEW monitoring.vw_pipeline_health AS
SELECT 
    DATE(start_time) as run_date,
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    AVG(duration_seconds) as avg_duration_seconds,
    MAX(rows_processed) as max_rows
FROM monitoring.pipeline_runs
GROUP BY DATE(start_time), pipeline_name;

CREATE VIEW monitoring.vw_data_quality_health AS
SELECT 
    check_date,
    table_name,
    COUNT(*) as total_checks,
    SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) as passed_checks,
    SUM(failed_count) as total_failures,
    AVG(failed_count * 1.0 / total_count) as avg_failure_rate
FROM monitoring.data_quality_log
GROUP BY check_date, table_name;
```

### 15.5 Fabric Terms Glossary

| Term | Definition |
|------|------------|
| **OneLake** | Single, unified, logical data lake for the entire organization  |
| **Lakehouse** | Data architecture combining data lake storage with SQL analytics endpoint |
| **Direct Lake** | Power BI mode that queries Delta files directly without import |
| **Medallion Architecture** | Bronze/Silver/Gold layered data design pattern |
| **Semantic Model** | Power BI dataset containing tables, relationships, measures |
| **CU (Capacity Unit)** | Compute and processing capacity in Fabric (billing unit) |
| **SQL Analytics Endpoint** | T-SQL interface for Lakehouse tables |
| **Shortcut** | Reference to external data without moving/copying |
| **Dataflow Gen2** | Low-code ETL using Power Query |
| **Spark Job Definition** | Batch Spark job execution |

---

## 16. Knowledge Check / Exercises

### Exercise 1: Build a Medallion Pipeline (45 minutes)

**Objective:** Create a complete Bronze → Silver → Gold pipeline for sales data.

**Dataset:** Sample sales CSV (provided)

**Tasks:**

1. **Create Workspace Structure**
   - Create bronze, silver, gold workspaces
   - Create Lakehouse in each

2. **Ingest to Bronze**
   - Create Dataflow to load CSV to bronze_lh
   - Add metadata columns
   - Partition by date

3. **Transform to Silver**
   - Write PySpark notebook to:
     - Deduplicate orders
     - Standardize status values
     - Join customer reference data
   - Write to silver_lh

4. **Create Gold Star Schema**
   - Create dim_customer (SCD Type 2)
   - Create dim_product
   - Create dim_date
   - Create fact_sales with surrogate keys

5. **Build Semantic Model**
   - Create relationships
   - Add measures (Total Sales, Profit Margin)
   - Set up Direct Lake connection

**Expected Outputs:**
- Bronze table with raw + metadata
- Silver table with cleaned data
- Gold star schema with 4 tables
- Semantic model with 5 measures
- Pipeline that orchestrates all steps

### Exercise 2: Performance Tuning (30 minutes)

**Objective:** Optimize a slow-running fact table.

**Scenario:** `gold_fact_sales` has 2B rows; queries are slow.

**Tasks:**

1. **Analyze Current Performance**
   - Run `DESCRIBE DETAIL gold_fact_sales`
   - Check table size and file count

2. **Implement Optimizations**
   - Run OPTIMIZE with Z-order on customer_key
   - Create aggregation table (`agg_daily_sales`)
   - Collect statistics

3. **Measure Improvement**
   ```sql
   -- Before optimization
   SELECT customer_region, SUM(sales_amount) 
   FROM gold_fact_sales f
   JOIN gold_dim_customer c ON f.customer_key = c.customer_key
   WHERE order_date >= '2026-01-01'
   GROUP BY customer_region;
   
   -- After: use aggregate
   SELECT customer_region, SUM(total_sales)
   FROM gold_agg_daily_sales
   WHERE date_key >= 20260101
   GROUP BY customer_region;
   ```

4. **Document Improvement**
   - Note query time before/after
   - Report file count reduction

### Exercise 3: Incident Response Simulation (20 minutes)

**Scenario:** Alert: "Pipeline Failure - silver_sales_transform"

**Given Error:**
```
org.apache.spark.sql.AnalysisException: 
Cannot write to 'silver_sales_orders' because it already contains data 
and the write operation is configured to 'error' mode.
```

**Tasks:**
1. Identify root cause
2. Determine fix
3. Write steps to resolve
4. Document preventive measures

**Expected Solution:**
- Root cause: Attempting to write without overwrite/append mode
- Fix: Change write mode to "overwrite" for full refresh or "append" for incremental
- Prevention: Parameterize write mode based on load type

---

## 17. Appendix

### 17.1 Official Documentation Links

- [Medallion Architecture in Fabric](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture) 
- [Lakehouse Tutorial](https://learn.microsoft.com/en-us/fabric/data-engineering/tutorial-lakehouse-introduction)
- [Direct Lake Mode](https://learn.microsoft.com/en-us/power-bi/enterprise/directlake-overview) 
- [Deployment Pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/understand-the-process) 
- [Fabric Capacity Metrics](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app)

### 17.2 Sample Queries Library

**Data Quality Checks:**
```sql
-- Check for NULLs in key columns
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer_key,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) as null_product_key
FROM gold_fact_sales;

-- Check for orphaned records
SELECT COUNT(*) as orphaned_sales
FROM gold_fact_sales f
LEFT JOIN gold_dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
```

**Usage Metrics:**
```sql
-- Top 10 queried tables
SELECT 
    table_name,
    COUNT(*) as query_count,
    AVG(duration_ms) as avg_duration_ms
FROM system.query_history
WHERE timestamp > now() - interval 7 days
GROUP BY table_name
ORDER BY query_count DESC
LIMIT 10;
```

### 17.3 Naming Conventions Quick Reference

| Item Type | Pattern | Example |
|-----------|---------|---------|
| Workspace | {env}-{domain}-{layer}-{region} | prd-sales-gold-weu |
| Lakehouse | {layer}_lh_{domain} | gold_lh_sales |
| Warehouse | {layer}_wh_{domain} | gold_wh_sales |
| Table | {layer}_{entity}_{type} | gold_fact_sales |
| Column | snake_case | customer_key |
| Pipeline | {source}_to_{target}_{desc} | bronze_to_silver_sales |
| Notebook | {layer}_{action}_{desc} | silver_transform_sales |
| Semantic Model | {domain}_{purpose}_model | sales_analytics_model |

### 17.4 Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-03-12 | Data Architecture Team | Initial release |
| 1.1 | 2026-03-15 | - | [Placeholder for updates] |

---

## 18. 1-Page Quickstart

```
╔══════════════════════════════════════════════════════════════════════════════╗
║         MICROSOFT FABRIC MEDALLION ARCHITECTURE - 1-PAGE QUICKSTART         ║
║                      Get from Raw Data to Power BI in 1 Hour                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

┌──────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: CREATE WORKSPACES & LAKEHOUSES (5 min)                              │
├──────────────────────────────────────────────────────────────────────────────┤
│ • Workspace: dev-sales-bronze → Lakehouse: bronze_lh                        │
│ • Workspace: dev-sales-silver → Lakehouse: silver_lh                        │
│ • Workspace: dev-sales-gold   → Lakehouse: gold_lh                          │
│                                                                              │
│   TIP: Use consistent naming across environments (dev/tst/prd)              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: INGEST TO BRONZE (10 min)                                           │
├──────────────────────────────────────────────────────────────────────────────┤
│ • Open bronze-workspace → New Dataflow Gen2                                 │
│ • Source: CSV/SQL Server/your data                                           │
│ • Add metadata columns:                                                      │
│     _ingestion_timestamp = current_timestamp()                              │
│     _source_system = 'source_name'                                          │
│ • Destination: bronze_lh → Table: bronze_sales_orders                       │
│ • Partition by: year(order_date)/month(order_date)/day(order_date)          │
│                                                                              │
│   TIP: Always use append mode for bronze (immutable landing zone)           │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: TRANSFORM TO SILVER (15 min)                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│ • Open silver-workspace → New Notebook                                      │
│ • PySpark code:                                                             │
│                                                                              │
│   from pyspark.sql.functions import col, to_date, row_number                │
│   from pyspark.sql.window import Window                                     │
│                                                                              │
│   # Read from bronze                                                        │
│   df = spark.table("bronze_lh.bronze_sales_orders")                         │
│                                                                              │
│   # Deduplicate (keep latest)                                               │
│   window = Window.partitionBy("order_id").orderBy(col("updated_at").desc()) │
│   deduped = df.withColumn("rn", row_number().over(window))                  │
│                .filter(col("rn") == 1).drop("rn")                           │
│                                                                              │
│   # Standardize & write to silver                                           │
│   deduped.write.format("delta").mode("overwrite")                           │
│          .saveAsTable("silver_lh.silver_sales_orders")                      │
│                                                                              │
│   TIP: Add data quality checks before writing                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: CREATE GOLD STAR SCHEMA (15 min)                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│ • Open gold-workspace → New SQL Query (via SQL Analytics Endpoint)          │
│                                                                              │
│   -- Create Date Dimension                                                  │
│   CREATE TABLE gold_dim_date AS                                             │
│   SELECT DISTINCT                                                           │
│       CAST(FORMAT(order_date, 'yyyyMMdd') AS INT) as date_key,              │
│       order_date as full_date,                                              │
│       YEAR(order_date) as year,                                             │
│       MONTH(order_date) as month                                            │
│   FROM silver_lh.silver_sales_orders;                                       │
│                                                                              │
│   -- Create Fact Table                                                      │
│   CREATE TABLE gold_fact_sales AS                                           │
│   SELECT                                                                    │
│       ROW_NUMBER() OVER (ORDER BY order_id) as sales_key,                   │
│       d.date_key,                                                           │
│       s.customer_id,                                                        │
│       s.order_amount as sales_amount                                        │
│   FROM silver_lh.silver_sales_orders s                                     │
│   JOIN gold_dim_date d ON CAST(s.order_date AS DATE) = d.full_date;         │
│                                                                              │
│   TIP: Add surrogate keys for all dimensions                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: CONNECT POWER BI VIA DIRECT LAKE (5 min)                           │
├──────────────────────────────────────────────────────────────────────────────┤
│ • Open gold-workspace → gold_lh                                             │
│ • Click "New Semantic Model"                                                │
│ • Select tables: gold_fact_sales, gold_dim_date                             │
│ • Verify relationships (auto-detected)                                      │
│ • Click "New Report" → Build visualizations                                 │
│                                                                              │
│   IMPORTANT: Check table storage mode = "Direct Lake" for best performance  │
│                                                                              │
│   PRO TIP: For large datasets (>100M rows), create aggregate tables         │
│   CREATE TABLE gold_agg_daily_sales AS ... GROUP BY date_key;               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ STEP 6: DEPLOY TO TEST/PROD (10 min)                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│ • In development workspace: New → Deployment Pipeline                       │
│ • Add stages: Development → Test → Production                               │
│ • Configure parameterization (server names, etc.)                           │
│ • Deploy to Test and validate                                               │
│ • Get approval → Deploy to Production                                       │
│                                                                              │
│   TIP: Use Git integration for version control                              │
│   Connect workspace to Azure DevOps repo                                    │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                 QUICK REFERENCE: COMMON T-SQL COMMANDS                      │
├──────────────────────────────────────────────────────────────────────────────┤
│ OPTIMIZE table ZORDER BY (column);          -- Optimize file layout         │
│ DESCRIBE HISTORY table;                      -- View table history          │
│ RESTORE TABLE table TO VERSION AS OF 123;    -- Time travel to version      │
│ VACUUM table RETAIN 168 HOURS;               -- Clean up old files          │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                          NEXT STEPS & RESOURCES                              │
│ • Official Docs: https://learn.microsoft.com/fabric                         │
│ • Cost Optimization: Set auto-pause for dev workspaces                      │
│ • Monitoring: Install Fabric Capacity Metrics app                           │
│ • Training: Microsoft Learn - Fabric Engineer Path                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 19. Exportable Checklist

```csv
Category,Task,Assigned To,Due Date,Status,Notes
Pre-Deployment,Capacity provisioned (F64 minimum),Infra Team,2026-03-15,Not Started,
Pre-Deployment,Azure AD groups created,Security Team,2026-03-15,Not Started,
Pre-Deployment,Naming conventions documented,Data Architect,2026-03-10,Completed,
Bronze Layer,Create bronze workspace,Data Engineer,2026-03-16,Not Started,
Bronze Layer,Create bronze lakehouse,Data Engineer,2026-03-16,Not Started,
Bronze Layer,Build ingestion pipeline (Dataflow),Data Engineer,2026-03-17,Not Started,
Bronze Layer,Add metadata columns,Data Engineer,2026-03-17,Not Started,
Bronze Layer,Configure partitioning (date),Data Engineer,2026-03-17,Not Started,
Bronze Layer,Set up monitoring alerts,Data Engineer,2026-03-18,Not Started,
Silver Layer,Create silver workspace,Data Engineer,2026-03-18,Not Started,
Silver Layer,Create silver lakehouse,Data Engineer,2026-03-18,Not Started,
Silver Layer,Implement deduplication logic,Data Engineer,2026-03-19,Not Started,
Silver Layer,Standardize data types,Data Engineer,2026-03-19,Not Started,
Silver Layer,Create dimension tables (SCD2),Data Engineer,2026-03-20,Not Started,
Silver Layer,Implement data quality checks,Data Engineer,2026-03-20,Not Started,
Silver Layer,Test referential integrity,Data Engineer,2026-03-21,Not Started,
Gold Layer,Create gold workspace,Data Engineer,2026-03-21,Not Started,
Gold Layer,Create gold lakehouse,Data Engineer,2026-03-21,Not Started,
Gold Layer,Design star schema,Data Architect,2026-03-22,Not Started,
Gold Layer,Create fact tables,Data Engineer,2026-03-22,Not Started,
Gold Layer,Create dimension tables,Data Engineer,2026-03-23,Not Started,
Gold Layer,Create aggregate tables,Data Engineer,2026-03-23,Not Started,
Gold Layer,Optimize with Z-ordering,Data Engineer,2026-03-24,Not Started,
Gold Layer,Collect statistics,Data Engineer,2026-03-24,Not Started,
Semantic Layer,Create semantic model,BI Developer,2026-03-24,Not Started,
Semantic Layer,Define relationships,BI Developer,2026-03-25,Not Started,
Semantic Layer,Create measures (DAX),BI Developer,2026-03-25,Not Started,
Semantic Layer,Configure RLS,BI Developer,2026-03-26,Not Started,
Semantic Layer,Test with Direct Lake,BI Developer,2026-03-26,Not Started,
Power BI,Build sample reports,BI Developer,2026-03-27,Not Started,
Power BI,Test performance,BI Developer,2026-03-27,Not Started,
Power BI,Publish to service,BI Developer,2026-03-28,Not Started,
CI/CD,Create deployment pipeline,DevOps Engineer,2026-03-28,Not Started,
CI/CD,Connect to Git,DevOps Engineer,2026-03-28,Not Started,
CI/CD,Configure parameterization,DevOps Engineer,2026-03-29,Not Started,
CI/CD,Set up automated tests,DevOps Engineer,2026-03-29,Not Started,
CI/CD,Deploy to Test,DevOps Engineer,2026-03-30,Not Started,
CI/CD,Deploy to Production,DevOps Engineer,2026-03-31,Not Started,
Monitoring,Install Capacity Metrics app,Admin,2026-03-15,Not Started,
Monitoring,Set up email alerts,Admin,2026-03-16,Not Started,
Monitoring,Create monitoring tables,Data Engineer,2026-03-20,Not Started,
Monitoring,Configure auto-pause for dev,Admin,2026-03-21,Not Started,
Security,Apply workspace RBAC,Security Team,2026-03-22,Not Started,
Security,Configure data masking,PII,Security Team,2026-03-23,Not Started,
Security,Test RLS with sample users,Security Team,2026-03-24,Not Started,
Documentation,Update data catalog,Data Steward,2026-03-25,Not Started,
Documentation,Create user guide,Data Steward,2026-03-26,Not Started,
Documentation,Train business users,Trainer,2026-03-27,Not Started,
Go-Live,Final smoke test,All,2026-04-01,Not Started,
Go-Live,Production cutover,All,2026-04-02,Not Started,
Go-Live,Post-launch monitoring,All,2026-04-03,Not Started,
```

---

*This guide is a living document. For questions, suggestions, or contributions, please contact the Data Architecture Team.*
```
