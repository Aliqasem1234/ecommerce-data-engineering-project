// Import Graph

LOAD CSV WITH HEADERS FROM 'file:///07_mongodb_modeling.csv' AS row
MERGE (c:Customer {id: row.customer_id})
MERGE (p:Product {id: row.product_id})
MERGE (c)-[:BOUGHT]->(p)


// Top 10 most purchased products


MATCH (c:Customer)-[:BOUGHT]->(p:Product) RETURN p.id AS product,count(*) AS purchases
ORDER BY purchases DESC LIMIT 10



// view Graph

MATCH (c:Customer)-[r:BOUGHT]->(p:Product) RETURN c,r,p LIMIT 50


// Products that are bought together
MATCH (p1:Product)<-[:BOUGHT]-(c:Customer)-[:BOUGHT]->(p2:Product) WHERE p1 <> p2 RETURN p1.id,p2.id, count (*) AS frequency
ORDER BY frequency DESC LIMIT 10



// Customer with similar purchases

 MATCH (c1:Customer)-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(c2:Customer) WHERE c1<>c2 RETURN c1.id,c2.id, count(p)AS common_products
ORDER BY common_products DESC


Product Recommendation


MATCH (c:Customer)-[:BOUGHT]->(p1:Product)<-[:BOUGHT]-(other:Customer)-[:BOUGHT]->(p2:Product)
WHERE p1 <> p2
RETURN p1.id AS bought_product,
       p2.id AS recommended_product,
       count(*) AS score
ORDER BY score DESC
LIMIT 10