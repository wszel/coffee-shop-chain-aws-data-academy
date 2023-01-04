/* Earn - transactions ratio of employees in last 7 days */
WITH t1 AS (
    SELECT 
        concat(s.stafffirstname, ' ', s.stafflastname) employee,
        SUM(sr.saleamount) summed_saleamount,
        so.salesoutletno salesoutletno,
        d.date DATE
    FROM staff s
        left join sales_reciept sr
        on sr.staff_id = s.staff_id
        left join customer c
        on c.customer_id = sr.customer_id
        left join date d
        on d.date_id = sr.date_id
        left join sales_outlet so
        on so.salesoutlet_id = sr.salesoutlet_id
    GROUP BY 1,3,4
    ORDER BY date DESC
    ),
t2 AS (
    SELECT
        summed_saleamount,
        employee,
        salesoutletno,
        DATE,
        RANK() OVER (PARTITION BY employee ORDER BY DATE DESC) AS date_rank
    FROM t1
    ),
t3 AS (
    SELECT
        employee,
        salesoutletno,
        summed_saleamount,
        DATE,
        1.0*SUM(summed_saleamount) OVER (PARTITION BY employee ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_sum
    FROM t2
    WHERE date_rank <=7
    ),
t4 AS (
    SELECT 
        concat(s.stafffirstname, ' ', s.stafflastname) employeee,
        COUNT(sr.transactionno) summed_transactionno,
        so.salesoutletno salesoutletno,
        d.date DATEE
    FROM staff s
        left join sales_reciept sr
        on sr.staff_id = s.staff_id
        left join customer c
        on c.customer_id = sr.customer_id
        left join date d
        on d.date_id = sr.date_id
        left join sales_outlet so
        on so.salesoutlet_id = sr.salesoutlet_id
    GROUP BY 1,3,4
    ORDER BY DATEE DESC
    ),
t5 AS (
    SELECT
        summed_transactionno,
        employeee,
        DATEE,
        RANK() OVER (PARTITION BY employeee ORDER BY DATEE DESC) AS date_rank
    FROM t4
    ),
t6 AS (
    SELECT
        employeee,
        summed_transactionno,
        DATEE,
        1.0*SUM(summed_transactionno) OVER (PARTITION BY employeee ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_transactionsno
    FROM t5
    WHERE date_rank <=7
    )
SELECT
    employee,
    salesoutletno,
--    cumulative_sum cumulative_earn,
--    cumulative_transactionsno cumulative_transactions_no,
    ROUND(cumulative_sum/cumulative_transactionsno, 2) ratio_earn_per_transaction_last_week,
    DATEE
FROM t3 JOIN t6 ON t6.employeee=t3.employee AND t6.DATEE=t3.DATE
ORDER BY DATEE;

/* Ratio of non-tea, non-coffee and non-chocholate product sales in all transactions */
WITH t1 AS (
    SELECT
        concat(s.stafffirstname, ' ', s.stafflastname) employee,
        so.salesoutletno salesoutletno,
        d.date DATE,
        COUNT(p.productcategory) counted_productcategory
    FROM staff s
        left join sales_reciept sr
        on sr.staff_id = s.staff_id
        left join customer c
        on c.customer_id = sr.customer_id
        left join date d
        on d.date_id = sr.date_id
        left join sales_outlet so
        on so.salesoutlet_id = sr.salesoutlet_id
        join product p
        on p.product_id = sr.product_id
    WHERE productcategory NOT IN ('Tea', 'Coffee', 'Drinking Chocolate')
    GROUP BY 1,2,3
    ),
t2 AS (
    SELECT
        counted_productcategory,
        employee,
        salesoutletno,
        DATE,
        RANK() OVER (PARTITION BY employee ORDER BY DATE DESC) AS date_rank
    FROM t1
    ),
t3 AS (
    SELECT
        employee,
        salesoutletno,
        counted_productcategory,
        DATE,
        1.0*SUM(counted_productcategory) OVER (PARTITION BY employee ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_sum_additional
    FROM t2
    WHERE date_rank <=7
    ),
t4 AS (
    SELECT
        concat(s.stafffirstname, ' ', s.stafflastname) employeee,
        so.salesoutletno salesoutletno,
        d.date DATEE,
        COUNT(p.productcategory) counted_productcategory
    FROM staff s
        left join sales_reciept sr
        on sr.staff_id = s.staff_id
        left join customer c
        on c.customer_id = sr.customer_id
        left join date d
        on d.date_id = sr.date_id
        left join sales_outlet so
        on so.salesoutlet_id = sr.salesoutlet_id
        join product p
        on p.product_id = sr.product_id
    GROUP BY 1,2,3
    ),
t5 AS (
    SELECT
        counted_productcategory,
        employeee,
        DATEE,
        RANK() OVER (PARTITION BY employeee ORDER BY DATEE DESC) AS date_rank
    FROM t4
    ),
t6 AS (
    SELECT
        employeee,
        counted_productcategory,
        DATEE,
        1.0*SUM(counted_productcategory) OVER (PARTITION BY employeee ORDER BY date_rank ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cumulative_sum_base
    FROM t5
    WHERE date_rank <=7
    )
SELECT
    employee,
    salesoutletno,
--    cumulative_sum_additional,
--    cumulative_sum_base,
    ROUND(cumulative_sum_additional/cumulative_sum_base, 2) ratio_non_coffee_tea_chocolate_to_all_sold,
    DATEE
FROM t3 JOIN t6 ON t6.employeee=t3.employee AND t6.DATEE=t3.DATE
ORDER BY DATEE;