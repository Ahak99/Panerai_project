# Panerai Scraper Project

A production-grade data pipeline designed to scrape the Panerai watch collection across multiple global markets and migrate the data into a Databricks Lakehouse environment for advanced analytics.

## 🏗️ Architecture Overview

The project employs a **Hybrid Cloud Strategy**, combining local stealth scraping with a cloud-native Medallion architecture.

### 1. Ingestion Layer (Local)
- **Stealth Scraping**: Uses `undetected-chromedriver` and `fake-useragent` to bypass bot detection.
- **Target Markets**: France, Japan, UK, USA, UAE, and Switzerland.
- **Data Transport**: Results are transferred from local Windows storage to **Databricks Volumes** via the Databricks REST API.

### 2. Lakehouse Pipeline (Databricks)
The pipeline is orchestrated as a **Data-Driven Workflow**, triggered automatically upon the arrival of new data in the Volume.

- **Bronze Layer**: Incremental ingestion via **Auto Loader (`cloudFiles`)** with checkpointing.
- **Silver Layer**: Data cleaning, normalization (EUR conversion), and URL validation using a **Validator Pattern**. Implements incremental upserts via watermarking.
- **Gold Layer**: Incremental transformation into a **Star Schema** (Dimensions and Fact tables) using SCD Type 1 for dimensions.
- **Analytics Layer**: Virtualized analysis layer using **SQL Views** to provide real-time accuracy for a PoC Dashboard without additional storage overhead.

## 🛡️ Governance & Environment
The workspace is built for enterprise-grade isolation:
- **Multi-Tier Isolation**: Separation between `dev` and `prod` environments is managed at the **Schema** level within the Unity Catalog.
- **Storage**: Uses **Databricks Volumes** for raw file landing and **Delta Lake** for all processed layers.
- **Event-Driven**: The pipeline only consumes compute resources when new data is detected in the Volume.

## 🚀 Quick Start

### Local Scraping
```bash
# Scrape all countries
python src/scraper.py

# Scrape specific countries
python src/scraper.py -c "USA,UK"
```

### Data Transfer
```bash
# Transfer all today's data
python src/transfer.py

# Transfer specific countries
python src/transfer.py -c "USA,UK"
```

### CI/CD Automation
This project is integrated with **GitHub Actions**. Every push to the `main` branch automatically triggers a synchronization with the Databricks workspace, ensuring the Lakehouse is always running the latest version of the pipeline.


## 🛠️ Tech Stack
- **Language**: Python 3.10+, PySpark
- **Scraping**: Selenium, `undetected-chromedriver`, `fake-useragent`, `pandas`
- **Platform**: Databricks (Lakeflow, Unity Catalog, Delta Lake)
- **Automation**: Windows Task Scheduler
- **Configuration**: YAML
