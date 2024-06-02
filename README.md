# Global_Electronics_Retailer_Hadoop

_Table of contents_
- [**1. Hadoop Ecosystem**](#1-hadoop-ecosystem)
- [**2. Data Architecture**](#2-data-architecture)
  * [2.1. Conceptual Architecture base on Fabric](#21-conceptual-architecture-base-on-fabric)
  * [2.2. Physical Architecture](#22-physical-architecture)
- [**3. Building End to End solutions**](#3-building-end-to-end-solutions)
  * [3.1. Dataset](#31-dataset)
  * [3.2. Orchestration (Data Catalog)](#32-orchestration-data-catalog)
  * [3.3. Building Master Pipeline](#33-building-master-pipeline)


# **1. Hadoop Ecosystem**


# **2. Data Architecture**

## 2.1. Conceptual Architecture base on Hadoop Ecosystem
![Items (1)](https://github.com/thanhphat2609/Global_Super_Store/assets/84914537/600e237e-01d7-4c09-891c-1551acfbc45e)

- **Data Source**: These include the various systems from which data is **extracted**, such as: Relational Database, File systems, SaaS applications, Real-time data.
- **Staging**: Extract data from Source into Files of Datalake (csv, parquet).
- **Data Warehouse**: Data in the data warehouse is organized according to a unified data model, which makes it easy to query and analyze.
- **Analytics**: This last step we will use tools and techniques to analyze the data in the data warehouse, such as: Power BI, Tableau, ..

## 2.2. Physical Architecture


- **Data Source** Layer: This layer is responsible for collecting and storing data from various sources, such as retail transaction data and customer data.

- **Data Transformation** Layer: Initially, data from the Source will be loaded into the Bronze layer (Files in Lakehouse) and stored in parquet format through a Pipeline (Source_to_Bronze). The data will then be validated and transformed, and stored in Delta format in the Silver layer (Tables in Lakehouse). Similar to the Bronze layer, there will be a Pipeline responsible for this task (Bronze_to_Silver). After obtaining data from the Silver layer, we proceed to create Dim and Fact tables in the Gold layer and upload them to Power BI using Direct Lake connectivity.

- **Reporting**: This layer is responsible for presenting data from the application layer to users in an understandable manner. This may include using web interfaces, mobile applications, or desktop applications.
