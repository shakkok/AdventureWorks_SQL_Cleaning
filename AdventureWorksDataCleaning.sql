--Calculate total revenue per month for 2013

SELECT YEAR(oh.OrderDate) AS order_year, MONTH(oh.OrderDate) AS order_month, 
CAST(ROUND(SUM(od.LineTotal), 2) AS DECIMAL(18,2)) AS  month_total
FROM AdventureWorks2022.Sales.SalesOrderHeader oh
JOIN AdventureWorks2022.Sales.SalesOrderDetail od
ON oh.SalesOrderID = od.SalesOrderID
WHERE YEAR(oh.OrderDate) = 2013
GROUP BY MONTH(oh.OrderDate), YEAR(oh.OrderDate)
ORDER BY MONTH(oh.OrderDate);



--Find average order value per customer

SELECT cu.CustomerID,  CAST(ROUND(AVG(soh.TotalDue), 2) AS DECIMAL(18,2)) AS  avg_total
FROM AdventureWorks2022.Sales.Customer AS cu
INNER JOIN AdventureWorks2022.Sales.SalesOrderHeader soh
ON cu.CustomerID = soh.CustomerID 
GROUP BY cu.CustomerID
ORDER BY avg_total DESC;


--Top 10 best-selling products by quantity


SELECT TOP 10 pp.Name AS product_name, pp.ProductID, SUM(srd.OrderQty) AS total_qty
FROM AdventureWorks2022.Sales.SalesOrderDetail srd
JOIN AdventureWorks2022.Production.Product pp
ON pp.ProductID = srd.ProductID
GROUP BY pp.ProductID, pp.Name
ORDER BY total_qty DESC;

--Customers inactive for the last 6 months

WITH LastOrder AS (
SELECT MAX(OrderDate) AS Max_Date
FROM AdventureWorks2022.Sales.SalesOrderHeader),

RecentOrders AS (
SELECT CustomerID
FROM AdventureWorks2022.Sales.SalesOrderHeader, LastOrder
WHERE OrderDate >= DATEADD(MONTH, -6, Max_Date))

SELECT c.CustomerID, c.PersonID, p.FirstName, p.LastName
FROM AdventureWorks2022.Sales.Customer c
JOIN AdventureWorks2022.Person.Person p
ON c.PersonID = p.BusinessEntityID
WHERE c.CustomerID NOT IN (SELECT CustomerID FROM RecentOrders)
ORDER BY c.CustomerID;

--Total profit per product category

SELECT pc.Name, CAST(ROUND(SUM(sod.LineTotal), 2) AS DECIMAL(18,2)) AS  total
FROM AdventureWorks2022.Production.ProductCategory pc
JOIN AdventureWorks2022.Production.ProductSubcategory psb
ON pc.ProductCategoryID = psb.ProductCategoryID
JOIN AdventureWorks2022.Production.Product p
ON p.ProductSubcategoryID = psb.ProductSubcategoryID
JOIN  AdventureWorks2022.Sales.SalesOrderDetail sod
ON sod.ProductID = p.ProductID
GROUP BY pc.Name
ORDER BY total;


--Total profit per product subcategory

SELECT psb.Name , CAST(ROUND(SUM(COALESCE(sod.LineTotal, 0)), 2) AS DECIMAL(18,2)) AS total
FROM AdventureWorks2022.Production.ProductSubcategory psb
LEFT JOIN AdventureWorks2022.Production.Product p
ON p.ProductSubcategoryID = psb.ProductSubcategoryID
LEFT JOIN AdventureWorks2022.Sales.SalesOrderDetail sod
ON sod.ProductID = p.ProductID
GROUP BY psb.Name
ORDER BY total DESC;

--Employees with the highest number of handled orders

SELECT p.FirstName, p.LastName, e.BusinessEntityID AS SalesPersonID, COUNT(soh.SalesOrderID) AS handled_orders_amount
FROM AdventureWorks2022.Sales.SalesOrderHeader soh
JOIN AdventureWorks2022.HumanResources.Employee e
ON e.BusinessEntityID = soh.SalesPersonID
JOIN AdventureWorks2022.Person.Person p
ON p.BusinessEntityID = e.BusinessEntityID
GROUP BY e.BusinessEntityID, p.FirstName, p.LastName
ORDER BY handled_orders_amount DESC;

-- Average order fulfillment time 

SELECT 
  ROUND(AVG(CAST(DATEDIFF(day, OrderDate, ShipDate) AS FLOAT)), 2) AS avg_delivery_days
FROM AdventureWorks2022.Sales.SalesOrderHeader
WHERE ShipDate IS NOT NULL;

--Average Order Value by Territory

SELECT st.TerritoryID, st.Name, YEAR(soh.OrderDate) AS yr, ROUND(AVG(soh.TotalDue), 2) AS avg_value
FROM AdventureWorks2022.Sales.SalesOrderHeader soh
JOIN AdventureWorks2022.Sales.SalesTerritory st
ON soh.TerritoryID = st.TerritoryID
WHERE ShipDate IS NOT NULL
GROUP BY st.TerritoryID, st.Name, YEAR(soh.OrderDate)
ORDER BY yr ,avg_value DESC;

-- Customer ranking by total order value 

WITH customer_sales AS (
    SELECT 
        c.CustomerID,
        ROUND(SUM(soh.TotalDue), 2) AS total,
        SUM(soh.TotalDue) * 1.0 / SUM(SUM(soh.TotalDue)) OVER () AS pct_of_total,
        SUM(SUM(soh.TotalDue)) OVER (ORDER BY SUM(soh.TotalDue) DESC) 
            / SUM(SUM(soh.TotalDue)) OVER () AS cumulative_pct
    FROM AdventureWorks2022.Sales.Customer c
    JOIN AdventureWorks2022.Sales.SalesOrderHeader soh
        ON soh.CustomerID = c.CustomerID
    GROUP BY c.CustomerID
)
SELECT *,
    CASE 
        WHEN cumulative_pct <= 0.7 THEN 'A'
        WHEN cumulative_pct <= 0.9 THEN 'B'
        ELSE 'C'
    END AS abc_class
FROM customer_sales
ORDER BY cumulative_pct ASC;

-- Invertory report by product and location

SELECT 
  l.LocationID,
  l.Name AS Location_name,
  p.ProductID,
  p.Name AS Product_name, 
  SUM(pin.Quantity) AS total_quantity
FROM AdventureWorks2022.Production.Location l
JOIN AdventureWorks2022.Production.ProductInventory pin
  ON pin.LocationID = l.LocationID
JOIN AdventureWorks2022.Production.Product p
   ON p.ProductID = pin.ProductID
GROUP BY 
  p.ProductID,
  p.Name,
  l.LocationID, 
  l.Name
HAVING SUM(pin.Quantity) > 0
ORDER BY 
  l.LocationID, 
  l.Name,
  p.ProductID;


  -- Product that have never been sold

  WITH sold_products AS (
  SELECT DISTINCT ProductID
  FROM AdventureWorks2022.Purchasing.PurchaseOrderDetail pod)
SELECT *
FROM AdventureWorks2022.Production.Product p
WHERE p.ProductID NOT IN (SELECT ProductID FROM sold_products);


-- Seasonal sales trend by category
 
SELECT 
DATEPART(YEAR, soh.OrderDate) AS order_year,
DATEPART(MONTH, soh.OrderDate) AS order_month,
ps.ProductSubcategoryID,
ps.Name AS sub_category_name,
pc.Name AS category_name,
CAST(ROUND(SUM(soh.TotalDue), 2) AS DECIMAL(18,2)) AS total_per_moth
FROM AdventureWorks2022.Sales.SalesOrderHeader soh
JOIN AdventureWorks2022.Sales.SalesOrderDetail sod
ON soh.SalesOrderID = sod.SalesOrderID
JOIN AdventureWorks2022.Production.Product p
ON p.ProductID = sod.ProductID
JOIN AdventureWorks2022.Production.ProductSubcategory ps
ON ps.ProductSubcategoryID = p.ProductSubcategoryID
JOIN AdventureWorks2022.Production.ProductCategory pc
ON pc.ProductCategoryID = ps.ProductCategoryID
GROUP BY DATEPART(YEAR, soh.OrderDate), 
DATEPART(MONTH, soh.OrderDate), 
ps.ProductSubcategoryID,
ps.Name,
pc.Name 
ORDER BY 
DATEPART(YEAR, soh.OrderDate), 
DATEPART(MONTH, soh.OrderDate),
total_per_moth DESC;


-- One time customer

WITH customer_count AS (
  SELECT 
    c.CustomerID,
    c.PersonID,
    c.StoreID,
    c.TerritoryID,
    c.AccountNumber,
    COUNT(DISTINCT soh.SalesOrderID) AS total_count,
    SUM(soh.SubTotal) AS total_amount
  FROM AdventureWorks2022.Sales.Customer c
  LEFT JOIN AdventureWorks2022.Sales.SalesOrderHeader soh
    ON c.CustomerID = soh.CustomerID
  GROUP BY 
    c.CustomerID, c.PersonID, c.StoreID, c.TerritoryID, c.AccountNumber
)
SELECT *
FROM customer_count
WHERE total_count = 1  
ORDER BY total_amount DESC;



-- Sales comparison by region 

WITH region_sales AS (
SELECT sp.StateProvinceID, 
sp.StateProvinceCode,
sp.CountryRegionCode,
ROUND(SUM(soh.TotalDue), 2) AS total_sales
FROM AdventureWorks2022.Person.StateProvince sp
JOIN AdventureWorks2022.Sales.SalesOrderHeader soh
ON sp.TerritoryID = soh.TerritoryID
GROUP BY sp.StateProvinceID, 
sp.StateProvinceCode,
sp.CountryRegionCode)

SELECT *,
ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rank
FROM region_sales
ORDER BY rank;


-- Average discount per product

SELECT p.Name,
sod.ProductID,
ROUND(AVG(COALESCE(sod.UnitPriceDiscount, 0)), 2) AS avg_discount,
SUM(sod.OrderQty) AS total_quantity_sold
FROM AdventureWorks2022.Production.Product p
JOIN AdventureWorks2022.Sales.SalesOrderDetail sod
ON p.ProductID = sod.ProductID
GROUP BY  p.Name,
sod.ProductID
ORDER BY avg_discount DESC;


-- Employyes who handled top revenue customers 

WITH salesperson_total AS (
    SELECT 
        soh.SalesPersonID,
        e.LoginID AS person_login,
        e.JobTitle AS person_job_title,
        ROUND(SUM(soh.TotalDue), 2) AS total_revenue
    FROM AdventureWorks2022.Sales.SalesOrderHeader soh
    JOIN AdventureWorks2022.HumanResources.Employee e
        ON soh.SalesPersonID = e.BusinessEntityID
    GROUP BY soh.SalesPersonID, e.LoginID, e.JobTitle
)
SELECT TOP 10 *
FROM salesperson_total
ORDER BY total_revenue DESC;

-- Frequently bought together products
WITH common_products AS (
SELECT a.ProductID AS orderid1,
b.ProductID AS orderid2
FROM AdventureWorks2022.Purchasing.PurchaseOrderDetail a
JOIN AdventureWorks2022.Purchasing.PurchaseOrderDetail b
ON a.PurchaseOrderID = b.PurchaseOrderID
WHERE a.ProductID <> b.ProductID
AND a.ProductID < b.ProductID)
 
SELECT orderid1,
orderid2,
COUNT(*) AS  times_bought_together
FROM common_products 
GROUP BY orderid1, orderid2
ORDER BY COUNT(*)  DESC;



