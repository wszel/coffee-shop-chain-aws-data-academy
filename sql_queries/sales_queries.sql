/* Number of transactions, sold products, sale amount by day of week, daytime and product category */
SELECT 
    p.productcategory as product_category, 
    d.dayofweekname as dayofweek,
    CASE 
        WHEN (sr.transactiontime) LIKE '06:%' 
        OR (sr.transactiontime) LIKE '07:%' 
        OR (sr.transactiontime) LIKE '08:%' 
        OR (sr.transactiontime) LIKE '09:%' 
        OR (sr.transactiontime) LIKE '10:%' 
        OR (sr.transactiontime) LIKE '11:%' THEN 'morning'
        WHEN (sr.transactiontime) LIKE'12:%' 
        OR (sr.transactiontime) LIKE'13:%' 
        OR (sr.transactiontime) LIKE'14:%' 
        OR (sr.transactiontime) LIKE'15:%' THEN 'afternoon'
        WHEN (sr.transactiontime) LIKE'16:%' 
        OR (sr.transactiontime) LIKE'17:%' 
        OR (sr.transactiontime) LIKE'18:%' 
        OR (sr.transactiontime) LIKE'19:%' 
        OR (sr.transactiontime) LIKE'20:%' THEN 'late-afternoon'
        WHEN (sr.transactiontime) LIKE'21:%' 
        OR (sr.transactiontime) LIKE'22:%' 
        OR (sr.transactiontime) LIKE'23:%' 
        OR (sr.transactiontime) LIKE'24:%' 
        OR (sr.transactiontime) LIKE'01:%' 
        OR (sr.transactiontime) LIKE'02:%' 
        OR (sr.transactiontime) LIKE'03:%'
        OR (sr.transactiontime) LIKE'04:%'
        OR (sr.transactiontime) LIKE'05:%' THEN 'night'
        END As day_time,
    COUNT(*) as no_of_transactions,
    SUM(sr.quantity) as no_of_products,
    ROUND(SUM(sr.saleamount), 2) as saleamount
FROM sales_reciept sr
JOIN date d USING (date_id)
JOIN product p USING (product_id)
GROUP BY 1, 2, 3
ORDER BY 2, 3;

/* Number of items sold from product category with cumulative sum over 7 days */

WITH t1 AS (
    SELECT 
        COUNT(p.productcategory) as NUMBER_OF_PRODUCT_SOLD_FROM_CATEGORY, 
        p.productcategory as CATEGORY_OF_PRODUCT, 
        d.date AS DATE
    FROM sales_reciept s
    LEFT JOIN product p USING (product_id)
    LEFT JOIN date d USING (date_id)
    GROUP BY   d.date, p.productcategory
    ),
t2 AS (
    SELECT
        NUMBER_OF_PRODUCT_SOLD_FROM_CATEGORY,
        CATEGORY_OF_PRODUCT,
        DATE,
        RANK() OVER (PARTITION BY CATEGORY_OF_PRODUCT ORDER BY DATE) AS date_rank
    FROM t1
    )
SELECT
    CATEGORY_OF_PRODUCT,
    NUMBER_OF_PRODUCT_SOLD_FROM_CATEGORY,
    DATE,
    SUM(NUMBER_OF_PRODUCT_SOLD_FROM_CATEGORY) OVER (PARTITION BY CATEGORY_OF_PRODUCT ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_number
FROM t2
ORDER BY 3 ASC;

/* Number of items sold from product group with cumulative sum over 7 days*/

WITH t1 AS (
    SELECT 
        COUNT(p.productgroup) as NUMBER_OF_PRODUCT_SOLD_FROM_GROUP, 
        p.productgroup as GROUP_OF_PRODUCT, 
        d.date AS DATE
    FROM sales_reciept s
    LEFT JOIN product p USING (product_id)
    LEFT JOIN date d USING (date_id)
    GROUP BY   d.date, p.productgroup
    ),
t2 AS (
    SELECT
        NUMBER_OF_PRODUCT_SOLD_FROM_GROUP,
        GROUP_OF_PRODUCT,
        DATE,
        RANK() OVER (PARTITION BY GROUP_OF_PRODUCT ORDER BY DATE) AS date_rank
    FROM t1
    )
SELECT
    GROUP_OF_PRODUCT,
    NUMBER_OF_PRODUCT_SOLD_FROM_GROUP,
    DATE,
    SUM(NUMBER_OF_PRODUCT_SOLD_FROM_GROUP) OVER (PARTITION BY GROUP_OF_PRODUCT ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_sum
FROM t2
ORDER BY 3 ASC;

/* Number of items sold from product type with cumulative sum over 7 days*/

WITH t1 AS (
    SELECT 
        COUNT(p.producttype) as NUMBER_OF_PRODUCT_SOLD_FROM_TYPE, 
        p.producttype as GROUP_OF_TYPE, 
        d.date AS DATE
    FROM sales_reciept s
    LEFT JOIN product p USING (product_id)
    LEFT JOIN date d USING (date_id)
    GROUP BY   d.date, p.producttype
    ORDER BY date ASC
    ),
t2 AS (
    SELECT
        NUMBER_OF_PRODUCT_SOLD_FROM_TYPE,
        GROUP_OF_TYPE,
        DATE,
        RANK() OVER (PARTITION BY GROUP_OF_TYPE ORDER BY DATE) AS date_rank
    FROM t1
    )
SELECT
    GROUP_OF_TYPE,
    NUMBER_OF_PRODUCT_SOLD_FROM_TYPE,
    DATE,
    SUM(NUMBER_OF_PRODUCT_SOLD_FROM_TYPE) OVER (PARTITION BY GROUP_OF_TYPE ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_sum
FROM t2
ORDER BY 3 ASC;

/* Number of items sold with cumulative sum over 7 days*/

WITH t1 AS (
    SELECT 
        p.productname PRODUCT_NAME,
        COUNT(p.productname) AS NUMBER_OF_PRODUCTS_SOLD, 
        d.date DATE
    FROM sales_reciept s
    LEFT JOIN product p USING (product_id)
    LEFT JOIN date d USING (date_id)
    GROUP BY  p.productname, d.date
    ),
t2 AS (
    SELECT
        NUMBER_OF_PRODUCTS_SOLD,
        PRODUCT_NAME,
        DATE,
        RANK() OVER (PARTITION BY PRODUCT_NAME ORDER BY DATE) AS date_rank
    FROM t1
    )
SELECT
    PRODUCT_NAME,
    NUMBER_OF_PRODUCTS_SOLD,
    DATE,
    SUM(NUMBER_OF_PRODUCTS_SOLD) OVER (PARTITION BY PRODUCT_NAME ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_sum
FROM t2
ORDER BY 3 ASC;

/* Most frequently bought volume by product category  and sale amount */

WITH t1 AS( 
    SELECT  d.monthname as month,
        p.productcategory as product_category, 
        p.productname as product_name, 
        p.volume as product_volume,
        p.unitofmeasure as unit_of_measure,
        p.productgroup as product_group,
        SUM(quantity) AS sold_amount
        
    from sales_reciept sr
    LEFT JOIN product p USING (product_id)
    LEFT JOIN date d USING (date_id)
    GROUP BY 1,2,3,4,5,6
    )
SELECT 
    distinct product_category, 
    CONCAT(CAST(product_volume as varchar),' ', unit_of_measure) as volume, 
    SUM(sold_amount) as total_sold
FROM t1
GROUP BY 1, 2
ORDER BY total_sold DESC;

/* Non-coffee products sold by daytime */

WITH t1 AS (
    SELECT 
        p.productcategory as product_category, 
        CASE 
            WHEN (sr.transactiontime) LIKE '06:%' OR (sr.transactiontime) LIKE '07:%' 
            OR (sr.transactiontime) LIKE '08:%' 
            OR (sr.transactiontime) LIKE '09:%' 
            OR (sr.transactiontime) LIKE '10:%' 
            OR (sr.transactiontime) LIKE '11:%' THEN 'morning'
            WHEN (sr.transactiontime) LIKE'12:%' OR (sr.transactiontime) LIKE'13:%' 
            OR (sr.transactiontime) LIKE'14:%' 
            OR (sr.transactiontime) LIKE'15:%' THEN 'afternoon'
            WHEN (sr.transactiontime) LIKE'16:%' OR (sr.transactiontime) LIKE'17:%' 
            OR (sr.transactiontime) LIKE'18:%' 
            OR (sr.transactiontime) LIKE'19:%' 
            OR (sr.transactiontime) LIKE'20:%' THEN 'late-afternoon'
            WHEN (sr.transactiontime) LIKE'21:%' OR (sr.transactiontime) LIKE'22:%' 
            OR (sr.transactiontime) LIKE'23:%' 
            OR (sr.transactiontime) LIKE'24:%' 
            OR (sr.transactiontime) LIKE'01:%' 
            OR (sr.transactiontime) LIKE'02:%' 
            OR (sr.transactiontime) LIKE'03:%'
            OR (sr.transactiontime) LIKE'04:%'
            OR (sr.transactiontime) LIKE'05:%' THEN 'night'
            END As day_time,
        SUM(sr.quantity) as quantity
    FROM sales_reciept sr
    JOIN date d USING (date_id)
    JOIN product p USING (product_id)
    WHERE NOT productcategory = 'Coffee' AND NOT productcategory = 'Coffee beans'
    GROUP BY 1, 2
    )
SELECT 
    day_time, 
    product_category, 
    SUM(quantity) as total_sold
FROM t1
GROUP BY 1, 2
ORDER BY day_time, total_sold desc;

/* Percentage of non-coffee products in all categories*/

SELECT  
    category,
    date, 
    1.0*SUM(quant) as quantity,
    1.0*ROUND(SUM(salamount), 2) as revenue,
    ROUND(100.00*SUM(quant)/SUM(SUM(quant)) OVER (PARTITION BY date), 2) AS percentage
FROM 
    (
    SELECT 
        product.productcategory as category,
        date,
        reciept_product_id,
        quant,
        salamount
    FROM product 
    JOIN
        (
        SELECT 
            date.date as date,
            sales_reciept.product_id as reciept_product_id,
            sales_reciept.quantity as quant,
            sales_reciept.saleamount as salamount
        FROM date
        JOIN sales_reciept ON "date"."date_id" = "sales_reciept"."date_id"
        ) a ON product.product_id = a.reciept_product_id
    WHERE NOT product.productcategory = 'Coffee' AND NOT product.productcategory = 'Coffee beans' 
    ) aa
    GROUP BY 1,2
    ORDER BY revenue DESC;

/* Netto gain from each day for each product */

SELECT  
    d.date as date,
    p.productname as product_name, 
    ROUND((SUM(currentretailprice)*1.0-SUM(currentwholesaleprice)*1.0), 2) AS net_revenue
FROM sales_reciept s
LEFT JOIN product p USING (product_id)
LEFT JOIN date d USING (date_id)
GROUP BY  1,2
ORDER BY date ASC;