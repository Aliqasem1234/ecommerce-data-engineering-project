# 🛒 E-Commerce Data Engineering & Graph Analytics Project

This project demonstrates a complete **Data Engineering pipeline** for an E-Commerce dataset using modern data tools.

The goal of the project is to clean, transform, analyze, and model the data using:

- Apache Spark (Databricks)
- MongoDB
- Neo4j Graph Database

The project also includes **Graph Analytics and Product Recommendation using Neo4j**.

---

# 📊 Architecture

The pipeline follows these steps:

Raw Data → Data Cleaning (Spark) → Data Modeling (MongoDB) → Graph Modeling (Neo4j) → Graph Analytics

---

# 🧰 Technologies Used

| Tool | Purpose |
|-----|------|
| **Databricks / Apache Spark** | Data Cleaning & Transformation |
| **MongoDB** | NoSQL Data Modeling |
| **MongoDB Aggregations** | Business Analytics |
| **Neo4j** | Graph Database Modeling |
| **Cypher** | Graph Queries |
| **GitHub** | Version Control |

---

# 📂 Project Structure


ecommerce-data-engineering-project
│
├── databricks
│   ├── 01_data_cleaning_customers.py
│   ├── 02_data_cleaning_orders.py
│   └── 03_data_cleaning_items.py
│   ├── 04_data_cleaning_products.py
│   ├── 05_data_integration.py
│   └── 06_mongobd_modeling.py
│
├── mongodb
│   ├── Customer_Product_ Networkjs.js
│   └── Customer _Retention_Analysiss.js
│   ├── Monthly_Revenue_Trendjs.ja
│   └── Product_Co_ Purchase_Analysisjs.js
│   ├── Revenue_by_Order_Status.js
│   └── top. fem_customers_by_revenue.js
│
├── neo4j
│   └── graph_analysis.cypher
│
├── data
│   └── customer_product_graph.csv
│
├── images
│   ├── databricks_pipeline.png
│   ├── mongodb_aggregation.png
│   └── neo4j_graph.png
│
└── README.md


---

# ⚡ Step 1 — Data Cleaning with Databricks (PySpark)

Raw CSV datasets were loaded into **Databricks** and cleaned using PySpark.

Cleaning included:

- Removing duplicates
- Standardizing text columns
- Fixing inconsistent date formats
- Handling null values
- Data normalization

Example:

```python
customers_df = customers_df.withColumn(
    "country",
    initcap(col("country"))
)

customers_df = customers_df.withColumn(
    "email",
    lower(col("email"))
)

🗄 Step 2 — Data Modeling in MongoDB
Cleaned data was modeled into MongoDB collections.
Example collections:
customers
orders
products

Orders were stored with embedded product items.
Example document:

{
  "order_id": 1023,
  "customer_id": 813,
  "items": [
    {
      "product_id": 412,
      "quantity": 2
    }
  ]
}

📈 Step 3 — MongoDB Aggregation Analysis
MongoDB aggregation pipelines were used to analyze the business data.
Example: Revenue by Order Status
db.orders.aggregate([
{
 $group: {
   _id: "$status",
   total_revenue: { $sum: "$total_revenue" },
   orders: { $sum: 1 }
 }
}
])

Example: Monthly Revenue Trend
db.orders.aggregate([
{
 $group: {
   _id: {
     year: { $year: "$order_date" },
     month: { $month: "$order_date" }
   },
   revenue: { $sum: "$total_revenue" }
 }
}
])

🧠 Step 4 — Graph Modeling with Neo4j
To perform Graph Analytics, the data was transformed into a graph structure.
Graph structure:
(Customer)-[:BOUGHT]->(Product)

Import data into Neo4j:
LOAD CSV WITH HEADERS FROM 'file:///customer_product_graph.csv' AS row
MERGE (c:Customer {id: row.customer_id})
MERGE (p:Product {id: row.product_id})
MERGE (c)-[:BOUGHT]->(p)

🔎 Step 5 — Graph Analytics
Using Cypher queries we performed several graph analyses.

Most Purchased Products
MATCH (p:Product)<-[:BOUGHT]-(c:Customer)
RETURN p.id, count(c) AS purchases
ORDER BY purchases DESC
LIMIT 10

Product Recommendation (Collaborative Filtering)
MATCH (c:Customer)-[:BOUGHT]->(p1:Product)<-[:BOUGHT]-(other:Customer)-[:BOUGHT]->(p2:Product)
WHERE p1 <> p2
RETURN p1.id, p2.id, count(*) AS score
ORDER BY score DESC
LIMIT 10

This query identifies products commonly purchased by similar customers.


Similar Customers
MATCH (c1:Customer)-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(c2:Customer)
WHERE c1 <> c2
RETURN c1.id, c2.id, count(p) AS common_products
ORDER BY common_products DESC
LIMIT 10

📸 Project Screenshots
Databricks Data Pipeline
MongoDB Aggregation Analysis
Neo4j Graph Network



🚀 Key Features of the Project
✔ Data Cleaning using PySpark
✔ NoSQL Data Modeling with MongoDB
✔ Business Analytics using MongoDB Aggregations
✔ Graph Modeling using Neo4j
✔ Graph Analysis
✔ Product Recommendation System
✔ Customer Similarity Detection

📚 Learning Outcomes
This project demonstrates:
Data Engineering workflows


NoSQL database design


Graph database analytics


Recommendation systems


Data pipeline architecture
