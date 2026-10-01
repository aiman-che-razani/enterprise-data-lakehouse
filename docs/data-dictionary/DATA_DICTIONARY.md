# RetailNexus Data Dictionary

**Version:** 1.0.0  
**Last Updated:** 2026-07  
**Maintained by:** Platform Engineering

This document describes every field in every domain across all medallion layers.

Legend: 🔑 Primary key · 🔗 Foreign key · 🔒 PII (tokenized in Silver) · ⏱ Timestamp · 📅 Date partition

---

## Table of Contents

- [Bronze Source Domains (14)](#bronze-source-domains)
- [Silver Cleansed Domains (14)](#silver-cleansed-domains)
- [Gold Dimensional Model](#gold-dimensional-model)
  - [Dimensions (8)](#dimensions)
  - [Facts (6)](#facts)
- [Pipeline Metadata Tables](#pipeline-metadata-tables)

---

## Bronze Source Domains

All Bronze tables share the following **metadata columns** appended during ingestion:

| Column | Type | Description |
|---|---|---|
| `_ingested_at` | timestamp | UTC wall-clock time the record was written |
| `_ingestion_date` | date | Partition date (`yyyy-MM-dd`) |
| `_batch_id` | string | UUID identifying the ingestion run |
| `_source_file` | string | S3 path of the raw source file |
| `_pipeline_version` | string | Semantic version of the pipeline code |

---

### customers

| Column | Type | Nullable | PII | Description |
|---|---|---|---|---|
| 🔑 `customer_id` | string | No | — | UUID from CRM |
| 🔒 `email` | string | Yes | Email | Contact email |
| 🔒 `phone` | string | Yes | Phone | Contact phone |
| `first_name` | string | Yes | — | Given name |
| `last_name` | string | Yes | — | Family name |
| 🔒 `address_line1` | string | Yes | Address | Street address |
| `city` | string | Yes | — | City |
| `state` | string | Yes | — | State/province |
| `country` | string | Yes | — | ISO 3166-1 alpha-2 |
| `postal_code` | string | Yes | — | Postal/ZIP code |
| 🔒 `date_of_birth` | date | Yes | DOB | Raw date of birth |
| `loyalty_tier` | string | Yes | — | BRONZE / SILVER / GOLD / PLATINUM |
| `loyalty_points` | integer | Yes | — | Current points balance |
| `acquisition_channel` | string | Yes | — | organic / paid / referral / social |
| ⏱ `created_at` | timestamp | No | — | Account creation time (UTC) |
| ⏱ `updated_at` | timestamp | Yes | — | Last profile update time (UTC) |

**SLA:** 26h · **Format:** JSON · **Volume:** 500K records · **Load:** Incremental CDC

---

### orders

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `order_id` | string | No | UUID order identifier |
| 🔗 `customer_id` | string | No | References `customers.customer_id` |
| 🔗 `store_id` | string | Yes | References `stores.store_id` (null = online) |
| `channel` | string | No | online / in-store / mobile |
| `status` | string | No | pending / confirmed / shipped / delivered / cancelled |
| `subtotal` | decimal(12,2) | No | Pre-discount, pre-tax total |
| `discount_amount` | decimal(12,2) | Yes | Order-level discount |
| `tax_amount` | decimal(12,2) | Yes | Applied tax |
| `total_amount` | decimal(12,2) | No | Final charged amount |
| `currency` | string | No | ISO 4217 (e.g. USD) |
| `payment_method` | string | Yes | card / paypal / bnpl / cash |
| ⏱ `ordered_at` | timestamp | No | Order placement time (UTC) |
| ⏱ `updated_at` | timestamp | Yes | Last status update time (UTC) |

**SLA:** 26h · **Format:** JSON · **Volume:** 1M records · **Load:** Streaming + batch

---

### order_lines

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `line_id` | string | No | UUID line item identifier |
| 🔗 `order_id` | string | No | References `orders.order_id` |
| 🔗 `product_id` | string | No | References `products.product_id` |
| `quantity` | integer | No | Units ordered |
| `unit_price` | decimal(10,2) | No | Catalogue price at order time |
| `discount_pct` | decimal(5,4) | Yes | Line-level discount (0.00–1.00) |
| `line_total` | decimal(12,2) | No | `quantity × unit_price × (1 - discount_pct)` |
| ⏱ `created_at` | timestamp | No | Line creation time |

**SLA:** 26h · **Format:** JSON · **Volume:** 2.5M records

---

### products

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `product_id` | string | No | UUID SKU identifier |
| 🔗 `supplier_id` | string | Yes | References `suppliers.supplier_id` |
| `sku` | string | No | Stock-keeping unit |
| `name` | string | No | Product display name |
| `category` | string | Yes | Hierarchy level 1 |
| `subcategory` | string | Yes | Hierarchy level 2 |
| `brand` | string | Yes | Brand name |
| `cost_price` | decimal(10,2) | Yes | Unit cost from supplier |
| `list_price` | decimal(10,2) | No | Standard retail price |
| `weight_kg` | decimal(8,3) | Yes | Shipping weight |
| `is_active` | boolean | No | Listed and available for sale |
| ⏱ `created_at` | timestamp | No | First listed |
| ⏱ `updated_at` | timestamp | Yes | Last modified |

**SLA:** 26h · **Format:** CSV · **Volume:** 50K records · **Load:** Full daily

---

### payments

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `payment_id` | string | No | UUID payment transaction |
| 🔗 `order_id` | string | No | References `orders.order_id` |
| `amount` | decimal(12,2) | No | Payment amount |
| `currency` | string | No | ISO 4217 |
| `method` | string | No | card / paypal / bnpl / cash |
| `status` | string | No | pending / completed / failed / refunded |
| `gateway` | string | Yes | Payment processor name |
| `gateway_reference` | string | Yes | External transaction ID (tokenised) |
| ⏱ `processed_at` | timestamp | No | Payment processing time (UTC) |
| ⏱ `settled_at` | timestamp | Yes | Settlement time (UTC) |

**SLA:** 26h · **Format:** JSON · **Volume:** 1M records · **Load:** Streaming

---

### inventory

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `inventory_id` | string | No | UUID stock record |
| 🔗 `product_id` | string | No | References `products.product_id` |
| 🔗 `store_id` | string | No | References `stores.store_id` |
| `quantity_on_hand` | integer | No | Current stock count |
| `quantity_reserved` | integer | Yes | Allocated to pending orders |
| `quantity_available` | integer | Yes | `on_hand - reserved` |
| `reorder_point` | integer | Yes | Trigger threshold for restocking |
| `reorder_quantity` | integer | Yes | Standard order quantity |
| ⏱ `snapshot_at` | timestamp | No | Stock snapshot time (UTC) |

**SLA:** 26h · **Format:** JSON · **Volume:** 500K records · **Load:** Streaming

---

### shipments

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `shipment_id` | string | No | UUID shipment identifier |
| 🔗 `order_id` | string | No | References `orders.order_id` |
| `carrier` | string | Yes | UPS / FedEx / USPS / DHL |
| `tracking_number` | string | Yes | Carrier tracking reference |
| `status` | string | No | pending / in_transit / delivered / returned |
| `origin_store_id` | string | Yes | Fulfilment store |
| ⏱ `shipped_at` | timestamp | Yes | Departure time (UTC) |
| ⏱ `estimated_delivery` | timestamp | Yes | Carrier ETA |
| ⏱ `delivered_at` | timestamp | Yes | Actual delivery time (UTC) |

**SLA:** 26h · **Format:** JSON · **Volume:** 800K records

---

### returns

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `return_id` | string | No | UUID return request |
| 🔗 `order_id` | string | No | References `orders.order_id` |
| 🔗 `product_id` | string | No | References `products.product_id` |
| `reason` | string | Yes | defective / wrong_item / changed_mind / other |
| `quantity` | integer | No | Units returned |
| `refund_amount` | decimal(12,2) | Yes | Amount to be refunded |
| `status` | string | No | pending / approved / processed / rejected |
| ⏱ `requested_at` | timestamp | No | Return request creation time |
| ⏱ `processed_at` | timestamp | Yes | Refund processing time |

**SLA:** 48h · **Format:** CSV · **Volume:** 150K records

---

### campaigns

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `campaign_id` | string | No | UUID campaign identifier |
| `name` | string | No | Campaign display name |
| `channel` | string | No | email / social / display / search / sms |
| `type` | string | No | awareness / conversion / retention / winback |
| `budget` | decimal(12,2) | Yes | Total allocated budget |
| `spend` | decimal(12,2) | Yes | Actual spend to date |
| `impressions` | integer | Yes | Total ad impressions |
| `clicks` | integer | Yes | Total clicks |
| `conversions` | integer | Yes | Attributed conversions |
| `revenue` | decimal(12,2) | Yes | Attributed revenue |
| `target_segment` | string | Yes | Audience segment code |
| 📅 `start_date` | date | No | Campaign start date |
| 📅 `end_date` | date | Yes | Campaign end date (null = ongoing) |

**SLA:** 48h · **Format:** CSV · **Volume:** 10K records

---

### clickstream

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `event_id` | string | No | UUID event identifier |
| `session_id` | string | No | Browser session UUID |
| 🔗 `customer_id` | string | Yes | Logged-in customer (null = anonymous) |
| `event_type` | string | No | page_view / click / add_to_cart / purchase / search |
| `page_url` | string | Yes | Full page URL |
| `referrer_url` | string | Yes | HTTP Referer header |
| `device_type` | string | Yes | desktop / mobile / tablet |
| `browser` | string | Yes | User-agent browser |
| `ip_address` | string | Yes | Masked to /24 subnet |
| `campaign_id` | string | Yes | UTM campaign reference |
| `product_id` | string | Yes | Viewed/cart product |
| ⏱ `event_at` | timestamp | No | Event time (UTC, ms precision) |

**SLA:** 2h · **Format:** JSON.gz · **Volume:** 5M records/day · **Load:** Kafka streaming

---

### reviews

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `review_id` | string | No | UUID review identifier |
| 🔗 `product_id` | string | No | Reviewed product |
| 🔗 `customer_id` | string | Yes | Reviewer (null = anonymous) |
| `rating` | integer | No | 1–5 star rating |
| `title` | string | Yes | Review headline |
| `body` | string | Yes | Review full text |
| `is_verified_purchase` | boolean | No | Verified buyer flag |
| `helpful_votes` | integer | Yes | Upvote count |
| ⏱ `submitted_at` | timestamp | No | Review submission time |

**SLA:** 48h · **Format:** JSON · **Volume:** 200K records

---

### iot_sensors

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `reading_id` | string | No | UUID reading identifier |
| `sensor_id` | string | No | Physical sensor identifier |
| 🔗 `store_id` | string | No | Store/warehouse location |
| `sensor_type` | string | No | temperature / humidity / motion / vibration |
| `value` | double | No | Raw sensor reading |
| `unit` | string | No | °C / % / boolean / mm/s² |
| `alert_triggered` | boolean | No | Threshold breach flag |
| ⏱ `recorded_at` | timestamp | No | Sensor reading time (UTC, sub-second) |

**SLA:** 2h · **Format:** JSON · **Volume:** 2M records/day · **Load:** Kafka streaming

---

### suppliers

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `supplier_id` | string | No | UUID supplier identifier |
| `name` | string | No | Legal company name |
| `country` | string | No | HQ country (ISO 3166-1 alpha-2) |
| `contact_email` | string | Yes | Procurement contact email |
| `payment_terms_days` | integer | Yes | Standard net payment days |
| `lead_time_days` | integer | Yes | Typical order lead time |
| `quality_score` | decimal(3,2) | Yes | Quality rating 0.00–1.00 |
| `is_preferred` | boolean | No | Preferred vendor flag |
| ⏱ `onboarded_at` | date | Yes | Supplier relationship start date |

**SLA:** 48h · **Format:** CSV · **Volume:** 2K records

---

### employees

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `employee_id` | string | No | UUID employee identifier |
| 🔗 `store_id` | string | No | Primary work location |
| 🔗 `manager_id` | string | Yes | Employee's direct manager |
| `first_name` | string | No | — |
| `last_name` | string | No | — |
| 🔒 `email` | string | No | Work email |
| `department` | string | No | ops / hr / finance / tech / sales |
| `role` | string | No | Job title |
| `employment_type` | string | No | full_time / part_time / contractor |
| `base_salary` | decimal(12,2) | Yes | Annual base salary |
| `bonus_pct` | decimal(5,4) | Yes | Target bonus as fraction |
| 📅 `hire_date` | date | No | Employment start date |
| 📅 `termination_date` | date | Yes | Employment end date (null = active) |

**SLA:** 48h · **Format:** CSV · **Volume:** 5K records

---

### finance

| Column | Type | Nullable | Description |
|---|---|---|---|
| 🔑 `transaction_id` | string | No | UUID GL transaction |
| `account_code` | string | No | Chart of accounts code |
| `account_name` | string | No | Account description |
| `account_type` | string | No | asset / liability / equity / revenue / expense |
| `debit_amount` | decimal(14,2) | Yes | Debit amount |
| `credit_amount` | decimal(14,2) | Yes | Credit amount |
| `net_amount` | decimal(14,2) | No | `credit - debit` |
| `currency` | string | No | ISO 4217 |
| `department` | string | Yes | Cost centre |
| `reference` | string | Yes | Invoice/PO reference |
| 📅 `transaction_date` | date | No | Accounting date |
| `fiscal_period` | string | No | YYYY-QQ (e.g. 2025-Q2) |
| ⏱ `posted_at` | timestamp | No | Journal posting time (UTC) |

**SLA:** 48h · **Format:** Parquet · **Volume:** 500K records

---

## Silver Cleansed Domains

Silver inherits all Bronze columns plus:

| Column | Type | Description |
|---|---|---|
| `_bronze_version` | bigint | Delta Lake version this record was read from |
| `_silver_processed_at` | timestamp | Silver processing time (UTC) |
| `_silver_date` | date | Silver processing date |
| `_batch_id` | string | Batch run UUID |
| `_pipeline_version` | string | Pipeline version |

**PII handling in Silver:**

| Bronze column | Silver treatment |
|---|---|
| `email` | Dropped; `email_hash` (SHA-256) added |
| `phone` | Column dropped entirely |
| `address_line1` | Column dropped entirely |
| `date_of_birth` | Dropped; `age_bucket` (10-year bands) added |

**SCD Type 2 columns (dimension tables only):**

| Column | Type | Description |
|---|---|---|
| `is_current` | boolean | True for the active record |
| `effective_from` | timestamp | Record became current |
| `effective_to` | timestamp | Record was superseded (null = current) |
| `_scd_hash` | string | SHA-256 of all SCD-tracked columns |
| `_silver_updated_at` | timestamp | Last Silver write time |

---

## Gold Dimensional Model

All Gold tables share:

| Column | Type | Description |
|---|---|---|
| `_gold_processed_at` | timestamp | Gold build time (UTC) |
| `_gold_date` | date | Gold build date |
| `_batch_id` | string | Batch that produced the row |
| `_pipeline_version` | string | Pipeline version |

Surrogate keys use `xxhash64(natural_key)` → `LongType` (deterministic, no lookup table).

---

### Dimensions

#### dim_date

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_date_id` | integer | N | 🔑 | YYYYMMDD integer (e.g. 20250601) |
| `full_date` | date | N | — | Calendar date |
| `day_of_week` | smallint | N | — | 1=Sunday … 7=Saturday (Spark `dayofweek`) |
| `day_name` | varchar(10) | N | — | Monday … Sunday |
| `day_of_month` | smallint | N | — | — |
| `day_of_year` | smallint | N | — | — |
| `week_of_year` | smallint | N | — | ISO week number |
| `month` | smallint | N | — | 1–12 |
| `month_name` | varchar(10) | N | — | January … December |
| `quarter` | smallint | N | — | 1–4 |
| `year` | smallint | N | — | Calendar year |
| `is_weekend` | boolean | N | — | Saturday or Sunday |
| `is_leap_year` | boolean | N | — | Leap year flag |
| `fiscal_quarter` | varchar(2) | N | — | e.g. Q2 |
| `fiscal_year` | smallint | N | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

**Range:** 2020-01-01 → 2030-12-31 (3,653 rows)

---

#### dim_customer

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_customer_id` | bigint | N | 🔑 | Surrogate key |
| `nk_customer_id` | varchar(36) | N | NK | Natural key from Silver |
| `email_hash` | varchar(64) | Y | — | SHA-256 of normalised email |
| `age_bucket` | varchar(10) | Y | — | 18-27 / 28-37 / … / 68+ |
| `segment` | varchar(20) | Y | — | — |
| `loyalty_tier` | varchar(20) | Y | — | BRONZE / SILVER / GOLD / PLATINUM |
| `loyalty_tier_rank` | smallint | Y | — | 1=BRONZE … 4=PLATINUM |
| `loyalty_points` | integer | Y | — | — |
| `country` | varchar(3) | Y | — | ISO 3166-1 alpha-2 |
| `state` | varchar(50) | Y | — | State/province |
| `channel_preference` | varchar(20) | Y | — | — |
| `is_email_verified` | boolean | Y | — | — |
| `is_sms_opted_in` | boolean | Y | — | — |
| `is_active` | boolean | Y | — | — |
| `years_as_customer` | float4 | Y | — | Tenure since `created_at` |
| `effective_from` | timestamp | Y | — | SCD2 valid-from |
| `effective_to` | timestamp | Y | — | SCD2 valid-to |
| `created_at` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### dim_product

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_product_id` | bigint | N | 🔑 | Surrogate key |
| `sk_supplier_id` | bigint | Y | SK | FK → `dim_supplier` |
| `nk_sku` | varchar(20) | N | NK | Natural key from Silver |
| `name` | varchar(200) | Y | — | Product name |
| `category_l1` | varchar(100) | Y | — | — |
| `category_l2` | varchar(100) | Y | — | — |
| `list_price` | decimal(10,2) | Y | — | Retail price |
| `cost_price` | decimal(10,2) | Y | — | Unit cost |
| `margin_amount` | decimal(10,2) | Y | — | `list_price - cost_price` |
| `margin_pct` | decimal(8,4) | Y | — | `margin / list_price` |
| `price_tier` | varchar(10) | Y | — | budget / mid-range / premium / luxury |
| `is_active` | boolean | Y | — | Active listing flag |
| `is_discontinued` | boolean | Y | — | — |
| `effective_from` | timestamp | Y | — | — |
| `effective_to` | timestamp | Y | — | — |
| `created_at` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### dim_store

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_store_id` | bigint | N | 🔑 | Surrogate key |
| `nk_store_id` | varchar(36) | N | NK | Natural key from Silver |
| `store_name` | varchar(200) | Y | — | — |
| `store_type` | varchar(50) | Y | — | flagship / standard / express / outlet |
| `city` | varchar(100) | Y | — | City |
| `state` | varchar(50) | Y | — | State/province |
| `country` | varchar(3) | Y | — | ISO 3166-1 alpha-2 |
| `latitude` | float8 | Y | — | — |
| `longitude` | float8 | Y | — | — |
| `sq_footage` | integer | Y | — | — |
| `num_employees` | integer | Y | — | — |
| `store_size_band` | varchar(10) | Y | — | small / medium / large / flagship |
| `sq_ft_per_employee` | decimal(8,1) | Y | — | Derived efficiency metric |
| `is_active` | boolean | Y | — | Operational flag |
| `opened_date` | date | Y | — | Store opening date |
| `closed_date` | date | Y | — | — |
| `effective_from` | timestamp | Y | — | — |
| `effective_to` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### dim_employee

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_employee_id` | bigint | N | 🔑 | Surrogate key |
| `sk_store_id` | bigint | Y | SK | FK → `dim_store` |
| `sk_manager_id` | bigint | Y | SK | Self-referential FK (null = top) |
| `nk_employee_id` | varchar(36) | N | NK | Natural key from Silver |
| `first_name_hash` | varchar(64) | Y | — | — |
| `last_name_hash` | varchar(64) | Y | — | — |
| `email_hash` | varchar(64) | Y | — | SHA-256 of work email |
| `department` | varchar(100) | Y | — | Org department |
| `job_title` | varchar(200) | Y | — | — |
| `level` | varchar(20) | Y | — | — |
| `hire_date` | date | Y | — | Employment start |
| `termination_date` | date | Y | — | — |
| `tenure_years` | decimal(5,1) | Y | — | Years since `hire_date` |
| `base_salary` | decimal(10,2) | Y | — | — |
| `bonus_pct` | decimal(5,4) | Y | — | — |
| `total_compensation` | decimal(10,2) | Y | — | `base_salary × (1 + bonus_pct)` |
| `is_active` | boolean | Y | — | Currently employed |
| `effective_from` | timestamp | Y | — | — |
| `effective_to` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### dim_supplier

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_supplier_id` | bigint | N | 🔑 | Surrogate key |
| `nk_supplier_id` | varchar(36) | N | NK | Natural key from Silver |
| `supplier_name` | varchar(200) | Y | — | — |
| `tier` | varchar(20) | Y | — | — |
| `on_time_rate` | decimal(5,4) | Y | — | — |
| `quality_score` | decimal(5,2) | Y | — | — |
| `supplier_score` | decimal(5,4) | Y | — | Composite quality + reliability score |
| `is_preferred` | boolean | Y | — | Preferred vendor flag |
| `payment_terms` | varchar(50) | Y | — | — |
| `country` | varchar(3) | Y | — | HQ country |
| `is_active` | boolean | Y | — | — |
| `effective_from` | timestamp | Y | — | — |
| `effective_to` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### dim_location

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_location_id` | bigint | N | 🔑 | country) |
| `nk_location_key` | varchar(300) | N | NK | Natural key from Silver |
| `city` | varchar(100) | Y | — | City name |
| `state` | varchar(50) | Y | — | State/province |
| `country` | varchar(3) | Y | — | ISO 3166-1 alpha-2 |
| `region` | varchar(20) | Y | — | North America / Europe / Asia Pacific / Other |
| `latitude` | float8 | Y | — | Latitude (stores only; null for customer cities) |
| `longitude` | float8 | Y | — | Longitude (stores only) |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### dim_promotion

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_promotion_id` | bigint | N | 🔑 | Surrogate key |
| `nk_campaign_id` | varchar(36) | N | NK | Natural key from Silver |
| `campaign_name` | varchar(200) | Y | — | — |
| `campaign_type` | varchar(50) | Y | — | — |
| `channel` | varchar(50) | Y | — | email / social / display / search / sms |
| `objective` | varchar(50) | Y | — | — |
| `start_date` | date | Y | — | Campaign start |
| `end_date` | date | Y | — | Campaign end (null = ongoing) |
| `budget` | decimal(12,2) | Y | — | Total budget |
| `campaign_duration_days` | integer | Y | — | `end_date - start_date` |
| `is_active` | boolean | Y | — | Currently running |
| `roi` | decimal(8,4) | Y | — | `(revenue - spend) / spend` |
| `performance_band` | varchar(20) | Y | — | poor / average / good / excellent |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

### Facts

All fact tables share:

| Column | Type | Description |
|---|---|---|
| `sk_date_id` | integer | 🔗 FK → `dim_date.sk_date_id` (YYYYMMDD) |
| `_gold_processed_at` | timestamp | Gold build time |

---

#### fact_sales

**Grain:** one row per order line item

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_customer_id` | bigint | N | SK | FK → `dim_customer` |
| `sk_product_id` | bigint | N | SK | FK → `dim_product` |
| `sk_store_id` | bigint | N | SK | FK -> `dim_store`; orders without a store hash to one shared key (never NULL) |
| `sk_date_id` | integer | N | SK | FK → `dim_date` (order date) |
| `sk_promotion_id` | bigint | Y | SK | FK → `dim_promotion` (null = no promo) |
| `nk_order_id` | varchar(36) | N | NK | Natural key from Silver |
| `nk_order_line_id` | varchar(36) | N | 🔑 | Natural key from Silver |
| `order_status` | varchar(20) | Y | — | — |
| `channel` | varchar(50) | Y | — | online / in-store / mobile |
| `sku` | varchar(20) | Y | — | — |
| `quantity` | integer | N | — | Units sold |
| `unit_price` | decimal(10,4) | N | — | Price at time of order |
| `effective_unit_price` | decimal(10,4) | Y | — | `unit_price × (1 - discount_pct)` |
| `line_total` | decimal(12,2) | N | — | — |
| `line_discount_amount` | decimal(10,2) | Y | — | — |
| `is_discounted` | boolean | Y | — | Any discount applied |
| `order_created_at` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### fact_inventory

**Grain:** one row per inventory snapshot (product × store × snapshot_at)

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_store_id` | bigint | N | SK | FK → `dim_store` |
| `sk_product_id` | bigint | N | SK | FK → `dim_product` |
| `sk_date_id` | integer | N | SK | FK → `dim_date` (snapshot date) |
| `nk_inventory_id` | varchar(36) | N | 🔑 | Natural key from Silver |
| `store_id` | varchar(36) | Y | — | — |
| `sku` | varchar(20) | Y | — | — |
| `snapshot_date` | date | N | — | — |
| `quantity_on_hand` | integer | Y | — | Current stock |
| `quantity_available` | integer | Y | — | Available to promise |
| `quantity_reserved` | integer | Y | — | Allocated to orders |
| `reorder_point` | integer | Y | — | Restock threshold |
| `max_stock_level` | integer | Y | — | — |
| `avg_daily_units` | decimal(8,2) | Y | — | — |
| `days_of_supply` | decimal(8,1) | Y | — | `on_hand / avg_daily_demand` |
| `abc_class` | varchar(1) | Y | — | — |
| `bin_location` | varchar(20) | Y | — | — |
| `stock_health` | varchar(20) | Y | — | overstock / healthy / low / out_of_stock |
| `needs_reorder` | boolean | Y | — | `on_hand ≤ reorder_point` |
| `updated_at` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### fact_marketing

**Grain:** one row per campaign per event date

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_promotion_id` | bigint | N | SK | FK → `dim_promotion` |
| `sk_date_id` | integer | N | SK | FK → `dim_date` |
| `nk_marketing_id` | varchar(100) | N | 🔑 | Natural key from Silver |
| `campaign_id` | varchar(36) | Y | — | — |
| `event_date` | date | N | — | — |
| `campaign_type` | varchar(50) | Y | — | — |
| `channel` | varchar(50) | Y | — | — |
| `sessions` | bigint | Y | — | Unique sessions attributed |
| `page_views` | bigint | Y | — | Total page views |
| `add_to_carts` | bigint | Y | — | — |
| `conversions` | bigint | Y | — | Daily conversions |
| `budget` | decimal(12,2) | Y | — | — |
| `revenue_attributed` | decimal(12,2) | Y | — | — |
| `ctr` | decimal(6,4) | Y | — | — |
| `conversion_rate` | decimal(6,4) | Y | — | `conversions / clicks` |
| `roi` | decimal(8,4) | Y | — | — |
| `performance_band` | varchar(20) | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### fact_returns

**Grain:** one row per return line

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_customer_id` | bigint | N | SK | FK → `dim_customer` |
| `sk_product_id` | bigint | Y | SK | FK → `dim_product` |
| `sk_store_id` | bigint | Y | SK | FK → `dim_store` |
| `sk_date_id` | integer | N | SK | FK → `dim_date` (request date) |
| `nk_return_id` | varchar(36) | N | 🔑 | Natural key from Silver |
| `nk_order_id` | varchar(36) | Y | NK | Natural key from Silver |
| `sku` | varchar(20) | Y | — | — |
| `reason` | varchar(100) | Y | — | Return reason code |
| `status` | varchar(20) | Y | — | — |
| `return_amount` | decimal(10,2) | Y | — | — |
| `refund_amount` | decimal(10,2) | Y | — | Refunded amount |
| `quantity_returned` | integer | Y | — | — |
| `days_to_process` | integer | Y | — | `processed_at - requested_at` days |
| `days_to_refund` | integer | Y | — | `processed_at - ordered_at` days |
| `is_fully_refunded` | boolean | Y | — | Full purchase amount returned |
| `initiated_at` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### fact_shipments

**Grain:** one row per shipment

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_store_id` | bigint | Y | SK | FK -> `dim_store` (NULL when the order has no store) |
| `sk_customer_id` | bigint | Y | SK | Surrogate key (xxhash64 of the natural key) |
| `sk_date_id` | integer | N | SK | FK -> `dim_date`: ship date, or the record-created date while unshipped |
| `nk_shipment_id` | varchar(36) | N | 🔑 | Natural key from Silver |
| `nk_order_id` | varchar(36) | Y | NK | Natural key from Silver |
| `carrier` | varchar(20) | Y | — | Carrier name |
| `service_level` | varchar(50) | Y | — | — |
| `tracking_number` | varchar(100) | Y | — | — |
| `shipping_cost` | decimal(8,2) | Y | — | — |
| `ship_date` | date | Y | — | — |
| `estimated_delivery_date` | date | Y | — | — |
| `actual_delivery_date` | date | Y | — | — |
| `days_in_transit` | integer | Y | — | `delivered_at - shipped_at` days |
| `is_on_time` | boolean | Y | — | `delivered_at ≤ estimated_delivery` |
| `delivery_variance_days` | integer | Y | — | `actual - estimated` (negative = early) |
| `status` | varchar(20) | Y | — | Current status |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

#### fact_finance

**Grain:** one row per GL transaction

| Column | Type | Null | Key | Description |
|---|---|---|---|---|
| `sk_date_id` | integer | N | SK | FK → `dim_date` (transaction date) |
| `sk_store_id` | bigint | Y | SK | Surrogate key (xxhash64 of the natural key) |
| `nk_transaction_id` | varchar(36) | N | 🔑 | Natural key from Silver |
| `transaction_type` | varchar(50) | Y | — | — |
| `gl_account` | varchar(10) | Y | — | — |
| `account_type` | varchar(20) | Y | — | asset / liability / equity / revenue / expense |
| `debit_amount` | decimal(14,2) | N | — | Debit amount |
| `credit_amount` | decimal(14,2) | N | — | Credit amount |
| `is_revenue` | boolean | Y | — | Revenue account flag |
| `is_expense` | boolean | Y | — | Expense account flag |
| `fiscal_quarter` | varchar(2) | Y | — | Q2 format |
| `transaction_date` | date | N | — | — |
| `description` | varchar(500) | Y | — | — |
| `posted_at` | timestamp | Y | — | — |
| `_gold_processed_at` | timestamp | Y | — | Gold build time (UTC) |
| `_gold_date` | date | Y | — | Gold build date |
| `_batch_id` | varchar(50) | Y | — | Batch that produced the row |
| `_pipeline_version` | varchar(20) | Y | — | Pipeline version |

---

## Pipeline Metadata Tables

Generated from `scripts/setup/init_postgres.sql` (the single definition of the operational schema).

### pipeline.watermarks

One row per domain (16) and per Gold table (14). Each layer writes only its own columns.

| Column | Type | Null | Notes |
|---|---|---|---|
| `domain` | varchar(128) | N | — |
| `bronze_last_ingest` | timestamptz | Y | — |
| `bronze_watermark` | timestamptz | Y | — |
| `bronze_rows_written` | bigint | Y | — |
| `bronze_last_status` | varchar(32) | Y | — |
| `silver_last_run` | timestamptz | Y | — |
| `silver_bronze_version` | bigint | Y | — |
| `silver_rows_written` | bigint | Y | — |
| `gold_last_run` | timestamptz | Y | — |
| `gold_rows_written` | bigint | Y | — |
| `redshift_last_load` | timestamptz | Y | — |
| `snowflake_last_load` | timestamptz | Y | — |
| `bigquery_last_load` | timestamptz | Y | — |
| `created_at` | timestamptz | N | — |
| `updated_at` | timestamptz | N | — |

### pipeline.runs

Airflow run audit log; `status` is lower-case.

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | bigserial | N | — |
| `run_id` | varchar(256) | N | — |
| `dag_id` | varchar(256) | N | — |
| `task_id` | varchar(256) | Y | — |
| `execution_date` | varchar(32) | Y | — |
| `layer` | varchar(16) | Y | — |
| `status` | varchar(32) | N | — |
| `rows_read` | bigint | Y | — |
| `rows_written` | bigint | Y | — |
| `rows_rejected` | bigint | Y | — |
| `error_message` | text | Y | — |
| `started_at` | timestamptz | N | — |
| `ended_at` | timestamptz | Y | — |

### pipeline.sla_misses

Written by the DQ freshness check and the SLA monitor DAG.

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | bigserial | N | — |
| `run_id` | varchar(256) | Y | — |
| `domain` | varchar(128) | N | — |
| `layer` | varchar(16) | Y | — |
| `metric` | varchar(256) | N | — |
| `actual_value` | double | Y | — |
| `sla_threshold` | double | Y | — |
| `message` | text | Y | — |
| `detected_at` | timestamptz | N | — |

### quality.check_results

`status` is a lower-case CheckStatus (passed / failed / warning / error).

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | bigserial | N | — |
| `run_id` | varchar(256) | N | — |
| `domain` | varchar(128) | N | — |
| `layer` | varchar(16) | N | — |
| `column_name` | varchar(128) | Y | — |
| `status` | varchar(16) | N | — |
| `metric_value` | double | Y | — |
| `threshold` | double | Y | — |
| `message` | text | Y | — |
| `rows_checked` | bigint | Y | — |
| `run_at` | timestamptz | N | — |

### quality.reports

One row per (run, domain, layer); `pass_rate` is a fraction in [0, 1]. Unique on that key.

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | bigserial | N | — |
| `run_id` | varchar(256) | N | — |
| `domain` | varchar(128) | N | — |
| `layer` | varchar(16) | N | — |
| `total_checks` | integer | N | — |
| `passed` | integer | N | — |
| `failed` | integer | N | — |
| `warnings` | integer | N | — |
| `errors` | integer | N | — |
| `pass_rate` | double | N | — |
| `run_at` | timestamptz | N | — |

### quality.contract_violations

Written by the Silver data-contract enforcer.

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | bigserial | N | — |
| `run_id` | varchar(256) | Y | — |
| `domain` | varchar(128) | N | — |
| `contract_version` | varchar(32) | N | — |
| `rule` | varchar(128) | N | — |
| `column_name` | varchar(128) | Y | — |
| `severity` | varchar(16) | N | — |
| `message` | text | Y | — |
| `detected_at` | timestamptz | N | — |

### catalog.tables

`table_name` is layer-qualified (`silver.orders`); unique with `schema_name`.

| Column | Type | Null | Notes |
|---|---|---|---|
| `table_id` | uuid | N | — |
| `schema_name` | varchar(64) | N | — |
| `table_name` | varchar(256) | N | — |
| `layer` | varchar(16) | N | — |
| `domain` | varchar(128) | N | — |
| `description` | text | Y | — |
| `owner_team` | varchar(128) | Y | — |
| `s3_location` | text | Y | — |
| `format` | varchar(32) | Y | — |
| `partition_columns` | text[] | Y | — |
| `row_count` | bigint | Y | — |
| `size_bytes` | bigint | Y | — |
| `schema_version` | varchar(32) | Y | — |
| `schema_json` | jsonb | Y | — |
| `tags` | text[] | Y | — |
| `created_at` | timestamptz | N | — |
| `updated_at` | timestamptz | N | — |

### catalog.columns

Replaced wholesale for a table on every catalog sync.

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | serial | N | — |
| `table_id` | uuid | N | — |
| `column_name` | varchar(128) | N | — |
| `data_type` | varchar(64) | N | — |
| `ordinal_position` | integer | Y | — |
| `is_nullable` | boolean | Y | — |
| `is_pii` | boolean | Y | — |
| `is_pci` | boolean | Y | — |
| `description` | text | Y | — |
| `example_values` | text[] | Y | — |
| `created_at` | timestamptz | N | — |

### catalog.lineage

Table-level edges; unique on (source_table, target_table, job_name).

| Column | Type | Null | Notes |
|---|---|---|---|
| `id` | serial | N | — |
| `source_table` | varchar(256) | N | — |
| `target_table` | varchar(256) | N | — |
| `transformation_type` | varchar(64) | Y | — |
| `job_name` | varchar(256) | N | NULLs would defeat the UNIQUE key below |
| `run_id` | varchar(256) | Y | — |
| `created_at` | timestamptz | N | — |

### api.api_keys

Only the SHA-256 hash of a key is stored. `permissions` holds scopes (`read:pipeline`, `read:quality`, `read:catalog`, or `*`).

| Column | Type | Null | Notes |
|---|---|---|---|
| `key_id` | uuid | N | — |
| `key_hash` | varchar(256) | N | — |
| `key_prefix` | varchar(16) | N | — |
| `client_name` | varchar(128) | N | — |
| `permissions` | jsonb | N | — |
| `is_active` | boolean | N | — |
| `expires_at` | timestamptz | Y | — |
| `last_used_at` | timestamptz | Y | — |
| `created_at` | timestamptz | N | — |
