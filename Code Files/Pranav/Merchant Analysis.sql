-- Orders per Hour
select c.name as merchant_name, EXTRACT(HOUR FROM o.created_at) AS hour, COUNT(o.id) AS order_count from `prod_cafe_20240417.orders` o
join `prod_cafe_20240417.companies` c on o.company_ord_fk = c.id
where c.name in ('Rook Coffee', 'Dalina')
group by merchant_name, hour;

-- Orders per Day
select c.name as merchant_name, EXTRACT(DAYOFWEEK FROM CAST(o.created_at AS TIMESTAMP)) - 1 AS day_of_the_week, COUNT(o.id) AS order_count from `prod_cafe_20240417.orders` o
join `prod_cafe_20240417.companies` c on o.company_ord_fk = c.id
where c.name in ('Rook Coffee', 'Dalina')
group by merchant_name, day_of_the_week;


-- Top Popular Products
select c.name as merchant_name, CASE WHEN ct.name = 'Pastry' THEN 'Pastries'
									 WHEN ct.name = 'DALINA Brunch all day' THEN 'Brunch' ELSE ct.name END as category_name, count(o.id) as total_orders, avg(pi.price) as average_price 
from `prod_cafe_20240417.companies` c
join `prod_cafe_20240417.orders` o on c.id = o.company_ord_fk
join `prod_cafe_20240417.order_items` oi on o.id = oi.order_fk
join `prod_cafe_20240417.product_items` pi on pi.id = oi.productItem_fk
join `prod_cafe_20240417.products` p on p.id = pi.product_fk
join `prod_cafe_20240417.categories` ct on ct.id = p.category_fk
where lower(p.name) not like '%test%' and lower(c.name) not like '%test%' and c.name IN ('Rook Coffee', 'Dalina')
group by c.name, ct.name
order by total_orders desc;


-- Cohort Analysis
WITH customer_first_order AS (
    SELECT
        user_id_fk AS user_id,
        company_ord_fk AS company_id,
        c.name AS merchant_name,
        MIN(DATE(o.created_at)) AS first_order_date
    FROM
        `prod_cafe_20240417.orders` o
    JOIN `prod_cafe_20240417.companies` c ON o.company_ord_fk = c.id
    WHERE
        c.name IN ('Rook Coffee', 'Dalina')
        AND LOWER(o.orderStatus) = 'completed'
    GROUP BY
        user_id, company_id, c.name
),
cohort_analysis AS (
    SELECT
        cfo.merchant_name,
        # EXTRACT(YEAR FROM cfo.first_order_date) AS cohort_year,
        # EXTRACT(MONTH FROM cfo.first_order_date) AS cohort_month,
        # EXTRACT(YEAR FROM DATE(o.created_at)) AS order_year,
        # EXTRACT(MONTH FROM DATE(o.created_at)) AS order_month,
        FORMAT_DATE("%Y-%m", DATE(cfo.first_order_date)) as cohort_period,
        FORMAT_DATE("%Y-%m", DATE(o.created_at)) as order_period,
        COUNT(DISTINCT o.user_id_fk) AS active_customers,
        SUM(o.calculated_total) AS total_revenue
    FROM
        `prod_cafe_20240417.orders` o
    JOIN customer_first_order cfo ON o.user_id_fk = cfo.user_id AND o.company_ord_fk = cfo.company_id
    WHERE
        LOWER(o.orderStatus) = 'completed'
    GROUP BY
        cfo.merchant_name, cohort_period,order_period
    ORDER BY
        cohort_period,order_period
)
SELECT
    merchant_name,
    case when merchant_name like '%Rook%' then 'Successful Merchant' else 'Unsuccessful Merchant' end as merchant,
    cohort_period,
    sum(active_customers) as active_customers,
    sum(total_revenue) as total_revenue
FROM
    cohort_analysis
WHERE
    cohort_period >= '2023-07'
GROUP BY
    merchant_name,
    cohort_period;