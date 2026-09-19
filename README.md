# Retail E-Commerce Supply Chain: Production ETL Data Pipeline

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white&style=flat-square)
![Pandas](https://img.shields.io/badge/Pandas-Data_Engineering-150458?logo=pandas&logoColor=white&style=flat-square)
![Apache Parquet](https://img.shields.io/badge/Apache_Parquet-Columnar_Storage-4084AC?style=flat-square)
![Status](https://img.shields.io/badge/Status-Completed-success?style=flat-square)
[![View Notebook](https://img.shields.io/badge/Jupyter-View_Notebook-F37626?logo=jupyter&style=flat-square)](retail_etl_pipeline.ipynb)

## 📌 Abstract
E-commerce retail operations require resilient data processing infrastructure to analyze fluctuations in consumer purchasing behavior across regional supply chains. In large enterprise retail networks like Walmart—where digital commerce accounts for over $80 billion in annual revenue (~13% of total enterprise sales)—seasonal milestones, promotional markdowns, and holiday events drive significant demand volatility.

To empower commercial supply chain planning and demand forecasting, this project establishes a modular, production-grade Extract, Transform, Load (ETL) pipeline in **Python** using `pandas`. The pipeline joins relational sales transactions with macroeconomic indicators, enforces statistical data imputation, engineers monthly cohort aggregations, and persists clean analytical datasets with automated file-system validation.

---

## 📂 Data Architecture & Schema
The pipeline consolidates weekly store department sales with external macroeconomic factors across 13 core operational attributes.

### Source Schema Definition
| Feature | Type | Business Definition |
| :--- | :--- | :--- |
| `Store_ID` | Integer | Retail store operational unit identifier |
| `Date` | String / Date | Start date of the recorded sales week (`YYYY-MM-DD`) |
| `Dept` | Integer | Store operating department classification code |
| `Weekly_Sales` | Float | Gross weekly department sales volume (USD) |
| `IsHoliday` | Integer | Binary holiday indicator flag (`1` = Holiday week, `0` = Standard week) |
| `Temperature` | Float | Regional average temperature on observation date (°F) |
| `Fuel_Price` | Float | Regional consumer fuel price per gallon (USD) |
| `CPI` | Float | Prevailing Consumer Price Index metric |
| `Unemployment` | Float | Prevailing local civilian unemployment rate |
| `MarkDown1`–`4` | Float | Anonymized promotional markdowns applied during sales cycles |
| `Size` | Integer | Total physical store footprint area |
| `Type` | String | Categorical store classification segment |

---

## 🛠️ Pipeline Architecture & Implementation
The ETL architecture is decoupled into modular stages with defensive validation barriers.

### 1. Relational & Columnar Extraction (`E`)
* Ingests columnar `.parquet` metadata (`extra_data.parquet`) and executes an inner relational merge on the primary transactional DataFrame across `index` keys.

### 2. Business Transformation & Data Cleansing (`T`)
* **Dual-Method Imputation**: Resolves missing values by imputing skewed sales (`Weekly_Sales`) with the statistical median, while normalizing macroeconomic variables (`CPI`, `Unemployment`) via their sample means.
* **Temporal Parsing**: Converts raw date strings into standard `datetime64` timestamps and extracts calendar `Month` numbers for monthly trend analysis.
* **Revenue Threshold Filtering**: Applies operational noise reduction by restricting downstream rows strictly to high-performing departments generating `Weekly_Sales > 10,000`.
* **Schema Pruning**: Drops redundant metadata, dimensional attributes, and auxiliary markdown variables (`Temperature`, `Fuel_Price`, `MarkDown1-4`, `Size`, `Type`, `Dept`) to optimize downstream memory footprint.

### 3. Metric Aggregation Engine (`A`)
* Generates an executive analytical view by grouping sanitized records by `Month`, computing `mean(Weekly_Sales)`, and formatting the output as `Avg_Sales` rounded to two decimal places.

### 4. Storage Loading & File-System Validation (`L & V`)
* Exports the full cleansed dataset (`clean_data.csv`) and the aggregated monthly benchmark (`agg_data.csv`) with index exclusion.
* Asserts delivery via an automated `validation()` stage using `os.path.exists()`, halting downstream execution if artifacts fail to materialize.

---

## 💻 Core Python Code

```python
import os
import pandas as pd


def extract(store_data: pd.DataFrame, extra_data: str) -> pd.DataFrame:
  """Extracts columnar parquet data and merges with tabular store transactions."""
  extra_df = pd.read_parquet(extra_data)
  merged_df = store_data.merge(extra_df, on="index")
  return merged_df


def transform(raw_data: pd.DataFrame) -> pd.DataFrame:
  """Sanitizes records, imputes missing values, and extracts seasonal features."""
  # Dual-method statistical imputation
  raw_data.fillna(
      {
          "Weekly_Sales": raw_data["Weekly_Sales"].median(),
          "CPI": raw_data["CPI"].mean(),
          "Unemployment": raw_data["Unemployment"].mean(),
      },
      inplace=True,
  )

  # Date parsing & month extraction
  raw_data["Date"] = pd.to_datetime(raw_data["Date"], format="%Y-%m-%d")
  raw_data["Month"] = raw_data["Date"].dt.month

  # Operational threshold filtering (> $10,000 weekly sales)
  raw_data = raw_data.loc[raw_data["Weekly_Sales"] > 10000, :]

  # Drop auxiliary & intermediate features
  raw_data = raw_data.drop(
      [
          "index",
          "Date",
          "Temperature",
          "Fuel_Price",
          "MarkDown1",
          "MarkDown2",
          "MarkDown3",
          "MarkDown4",
          "Dept",
          "Size",
          "Type",
      ],
      axis=1,
  )

  return raw_data


def avg_weekly_sales_per_month(clean_data: pd.DataFrame) -> pd.DataFrame:
  """Aggregates average weekly sales grouped by calendar month."""
  data = clean_data[["Month", "Weekly_Sales"]]
  agg_data = data.groupby("Month")[["Weekly_Sales"]].mean()
  agg_data = agg_data.rename(columns={"Weekly_Sales": "Avg_Sales"})
  agg_data = agg_data.reset_index()
  agg_data["Avg_Sales"] = agg_data["Avg_Sales"].round(2)
  return agg_data


def load(
    full_data: pd.DataFrame,
    full_data_file_path: str,
    agg_data: pd.DataFrame,
    agg_data_file_path: str,
) -> None:
  """Persists full cleansed and aggregated datasets to CSV storage."""
  full_data.to_csv(full_data_file_path, index=False)
  agg_data.to_csv(agg_data_file_path, index=False)


def validation(file_path: str) -> None:
  """Validates successful file-system artifact persistence."""
  if not os.path.exists(file_path):
    raise Exception(
        f"Validation Failed: The file at '{file_path}' does not exist."
    )


# End-to-End Pipeline Execution
if __name__ == "__main__":
  merged_df = extract(grocery_sales, "extra_data.parquet")
  clean_data = transform(merged_df)
  agg_data = avg_weekly_sales_per_month(clean_data)

  load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")

  validation("clean_data.csv")
  validation("agg_data.csv")
```
---
*© 2026 Ryan Tang.*
