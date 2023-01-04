
/* AGE of Customers*/
SELECT
    COUNT(*) "Number of Customers",
    (d.year - c.birthyear) as "Age"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
LEFT JOIN date d using (date_id)
GROUP BY 2
ORDER BY 1 DESC;

/*AGE GROUP of Customers*/
SELECT
    COUNT(*) "Number of Customers",
    CASE
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) < 20 THEN 'UNDER 20'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 20 AND 30 THEN '20-30'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 30 AND 40 THEN '30-40'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 40 AND 50 THEN '40-50'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 50 AND 50 THEN '50-60'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) > 20 THEN 'OVER 60'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) IS NULL THEN 'NO INFO'
    END AS "Age Groups"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
LEFT JOIN date d using (date_id)
GROUP BY 2
ORDER BY 1 DESC;

/* Number of transactions with registered clients */
SELECT
    (SELECT COUNT(*) FROM sales_reciept sr
        LEFT JOIN customer c using (customer_id) 
            WHERE c.customerfirstname != 'NULL' 
            OR c.customersurname != 'NULL' 
            OR c.loyaltycardnumber != 'NULL' 
            OR c.customeremail != 'NULL') as number_of_transactions_with_registered_customers,
    COUNT(sr.transactionno) as total_number_of_transactions
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
;

/* Loyalty Card Holders */
SELECT 
    COUNT(IF(loyaltycardnumber = 'NULL', NULL, 1)) as "Loyaty Card Holders",
    COUNT(customerno) AS "Number of Registered Customers"
FROM customer;


/* The number of transactions can be min or max*/
SELECT
    COUNT(sr.transactionno) AS "Number of transactions",
    c.loyaltycardnumber AS "Customer Loyalty Card Number"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
GROUP BY 2
ORDER BY 1 DESC;

/* The number of products bought can be min or max */
SELECT
    SUM(sr.quantity) AS "Number of products bought",
    c.loyaltycardnumber AS "Customer Loyalty Card Number"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
GROUP BY 2
ORDER BY 1 DESC;

/* The number of products bought and transactions made by gender */
SELECT
    SUM(sr.quantity) AS "Number of products bought",
    COUNT(sr.transactionno) AS "Number of transactions",
    c.gender AS "Gender"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
GROUP BY 3;

/* The number of products bought and transactions made by age group */
SELECT
    COUNT(*) "Number of Customers",
    CASE
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) < 20 THEN 'UNDER 20'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 20 AND 30 THEN '20-30'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 30 AND 40 THEN '30-40'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 40 AND 50 THEN '40-50'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 50 AND 50 THEN '50-60'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) > 20 THEN 'OVER 60'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) IS NULL THEN 'NO INFO'
    END AS "Age Groups",
    SUM(sr.quantity) AS "Number of products bought",
    COUNT(sr.transactionno) AS "Number of transactions"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
LEFT JOIN date d using (date_id)
GROUP BY 2;

/* Items on promotion and transactions by gender*/
SELECT
    SUM(sr.quantity) AS "Number of products bought",
    COUNT(sr.transactionno) AS "Number of transactions",
    c.gender AS "Gender"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
LEFT JOIN product p using(product_id)
WHERE p.ispromoitem = 'Y'
GROUP BY 3;

/* The number of products bought and transactions made by gender and product category*/
SELECT
    SUM(sr.quantity) AS "Number of products bought",
    COUNT(sr.transactionno) AS "Number of transactions",
    p.productcategory AS "Product Category",
    c.gender AS "Gender"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
LEFT JOIN product p using(product_id)
GROUP BY 3, 4;

/* The number of products bought and transactions made by age group and product category*/
SELECT
    CASE
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) < 20 THEN 'UNDER 20'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 20 AND 30 THEN '20-30'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 30 AND 40 THEN '30-40'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 40 AND 50 THEN '40-50'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 50 AND 50 THEN '50-60'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) > 20 THEN 'OVER 60'
        WHEN (cast(d.year as int) - cast(c.birthyear as int)) IS NULL THEN 'NO INFO'
    END AS "Age Groups",
    SUM(sr.quantity) AS "Number of products bought",
    COUNT(sr.transactionno) AS "Number of transactions",
    p.productcategory AS "Product Category"
FROM sales_reciept sr
LEFT JOIN customer c using (customer_id)
LEFT JOIN date d using (date_id)
LEFT JOIN product p using (product_id)
GROUP BY 1, 4;