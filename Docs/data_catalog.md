# 📊 Gold Layer Data Catalog

## Overview
The **Gold layer** contains analytics-ready views built on top of the Silver layer.  
These views follow a **star schema** design and are optimized for reporting, BI tools, and business analysis.

**Gold Objects:**
- `gold.dem_customers` – Customer Dimension
- `gold.dem_products` – Product Dimension
- `gold.fact_sales` – Sales Fact Table

---

## ⭐ Star Schema Design

- **Dimensions**
  - Customers
  - Products
- **Fact**
  - Sales transactions

The fact table (`gold.fact_sales`) connects to dimensions using **surrogate keys**.

---

## 🧍 View: `gold.dem_customers`

### Description
Customer dimension combining CRM customer data with ERP demographic and location information.

### Grain
**One row per customer**

### Primary Key
- `customer_key` (surrogate key)

### Source Tables
- `silver.crm_cust_info` (master customer data)
- `silver.erp_cust_az12` (birth date & gender)
- `silver.erp_Loc_A101` (country)

### Business Rules
- CRM is treated as the **master source for gender**
- If CRM gender is `Unknown`, ERP gender is used
- Country comes from ERP location data

### Columns

| Column Name | Description |
|------------|-------------|
| `customer_key` | Surrogate key generated using `ROW_NUMBER()` |
| `customer_id` | CRM customer ID |
| `customer_number` | Business customer key |
| `first_name` | Customer first name |
| `last_name` | Customer last name |
| `country` | Customer country |
| `marital_status` | Customer marital status |
| `gender` | Standardized gender (CRM preferred) |
| `create_date` | Customer record creation date |
| `birth_date` | Customer birth date |

---

## 📦 View: `gold.dem_products`

### Description
Product dimension combining CRM product master data with ERP product categories.

### Grain
**One row per active product**

### Primary Key
- `product_key` (surrogate key)

### Source Tables
- `silver.crm_prd_info`
- `silver.erp_px_cat_g1v2`

### Filters
- Only **active products** are included  
  (`prd_end_dt IS NULL`)

### Columns

| Column Name | Description |
|------------|-------------|
| `product_key` | Surrogate product key |
| `product_id` | CRM product ID |
| `category_id` | Product category ID |
| `product_number` | Business product key |
| `product_name` | Product name |
| `product_cost` | Product cost |
| `product_line` | Product line (mapped value) |
| `product_start_date` | Product start date |
| `product_category` | Product category name |
| `product_sub_category` | Product sub-category name |
| `maintenance` | Maintenance classification |

---

## 💰 View: `gold.fact_sales`

### Description
Fact table storing sales transactions linked to customer and product dimensions.

### Grain
**One row per sales transaction**

### Source Tables
- `silver.crm_sales_details`
- `gold.dem_customers`
- `gold.dem_products`

### Foreign Keys
- `customer_key` → `gold.dem_customers`
- `product_key` → `gold.dem_products`

### Columns

| Column Name | Description |
|------------|-------------|
| `sls_ord_num` | Sales order number |
| `product_key` | Foreign key to product dimension |
| `customer_key` | Foreign key to customer dimension |
| `sls_order_dt` | Order date |
| `sls_ship_dt` | Shipping date |
| `sls_due_dt` | Due date |
| `sls_sales` | Sales amount |
| `sls_quantity` | Quantity sold |
| `sls_price` | Unit price |

---

## ✅ Key Design Notes
- Surrogate keys are generated using `ROW_NUMBER()`
- Dimensions are **conformed** and reused across facts
- Data is fully cleaned and standardized in Silver before reaching Gold
- Gold layer is optimized for:
  - BI tools (Power BI / Tableau)
  - Reporting
  - Aggregations & analytics

---

## 🔄 Data Lineage Summary


