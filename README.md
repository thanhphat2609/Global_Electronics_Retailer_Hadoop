# End to End project Global Electronics Retailer Analysis with Hadoop Ecosystem using 2-tier Architecture.

_Table of contents_
- [**1. Hadoop Ecosystem**](#1-hadoop-ecosystem)
- [**2. Data Architecture**](#2-data-architecture)
  * [2.1. Conceptual Architecture base on Fabric](#21-conceptual-architecture-base-on-fabric)
  * [2.2. Physical Architecture](#22-physical-architecture)
- [**3. Building End to End solutions**](#3-building-end-to-end-solutions)
  * [3.1. Dataset Diagram](#31-dataset)
  * [3.2. Building HDFS](#32-building-hdfs)
  * [3.3. Building Python Files for Pipeline](#33-building-python-files-for-pipeline)


# **1. Hadoop Ecosystem**
![Hadoop_Ecosystem](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop/assets/84914537/2ca1841c-6829-4402-8363-4d2debfa0f06)

Introduce some tools for project:

- **Hadoop**: Hadoop is an open-source framework designed for distributed storage and processing of large datasets across clusters of computers using simple programming models. It provides a distributed file system (HDFS) and a framework for the processing of big data using the MapReduce programming model.

- **HDFS** (Hadoop Distributed File System): HDFS is a distributed file system designed to store large volumes of data reliably and efficiently across multiple machines. It is the primary storage system used by Hadoop, providing high throughput access to application data.

- **Apache Spark**: Apache Spark is an open-source, distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. Spark's in-memory computing capabilities make it well-suited for iterative algorithms and interactive data analysis.

- **Apache Hive**: Hive is a data warehouse infrastructure built on top of Hadoop that provides data summarization, query, and analysis. It enables querying and managing large datasets stored in Hadoop's HDFS using a SQL-like language called HiveQL.

- **Apache Superset**: Apache Superset is an open-source business intelligence (BI) tool that offers a rich set of visualization options and features for exploring and analyzing data. It supports a wide range of data sources and allows users to create interactive dashboards and data exploration workflows.


# **2. Data Architecture**

## 2.1. Conceptual Architecture base on Hadoop Ecosystem
![Items (1)](https://github.com/thanhphat2609/Global_Super_Store/assets/84914537/600e237e-01d7-4c09-891c-1551acfbc45e)

- **Data Source**: These include the various systems from which data is **extracted**, such as: Relational Database, File systems, SaaS applications, Real-time data.
- **Staging**: Extract data from Source into Files of Datalake (csv, parquet).
- **Data Warehouse**: Data in the data warehouse is organized according to a unified data model, which makes it easy to query and analyze.
- **Analytics**: This last step we will use tools and techniques to analyze the data in the data warehouse, such as: Power BI, Tableau, ..

## 2.2. Physical Architecture
![f2421eb3-7efc-422c-b0de-926effb11f81](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop/assets/84914537/7d362cdf-e7d3-490a-8602-1c9d467c8bb1)


- **Data Source** Layer: This layer is responsible for collecting and storing data from various sources, such as retail transaction data and customer data.

- **Data Transformation** Layer: Initially, data from the Source will be loaded into the Datalake layer (datalake in HDFS) and stored in parquet format through a reading data from MySQL by Apache Spark (Source_to_Datalake.py). Similar to the Datalae layer, there will be a Python Files responsible for transformation data from Datalake and then create Data Warehouse at Apache Hive (warehouse in HDFS)(Datalake_to_Datawarehouse.py).

- **Reporting**: This layer is responsible for presenting data from the application layer to users in an understandable manner. This may include using web interfaces, mobile applications, or desktop applications.

# **3. Building End to End solutions**

## 3.1. Dataset Diagram
![DataSetDiagram](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop/assets/84914537/e34766d2-8b75-4e32-8445-7bc4dcbd610e)


## 3.2. Building HDFS
![Mh6O-sC5](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop/assets/84914537/fd9d5eb2-a874-44bd-ab96-b1b7215835b5)


## 3.3. Building Python Files for Pipeline
