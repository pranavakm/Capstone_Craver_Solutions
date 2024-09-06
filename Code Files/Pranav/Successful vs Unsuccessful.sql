-- Successful merchants (Craver revenue vs Non-Craver revenue)
WITH square_merchant_ids AS (
  SELECT 
    merchant_id,
    craver_merchant_name,
    COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) AS craver_merchant_square_id_resolved
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_locations`
  WHERE COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) IS NOT NULL
  GROUP BY 
    merchant_id,
    craver_merchant_name,
    craver_merchant_square_id_resolved
),
merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name, 
    companies.id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned,
    COALESCE(square_merchant_ids.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON companies.payment_credentials_id = square_merchant_ids.craver_merchant_square_id_resolved
  WHERE
    companies.payment_integration = "SQUARE"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, square_merchant_id, companies.is_disabled
),
temp_craver_order_count_by_square_location AS (
  SELECT 
    square_locations.id AS square_location_id,
    square_locations.merchant_id AS square_merchant_id,
    square_locations.business_name AS square_merchant_name,
    COALESCE(COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.id) ELSE NULL END), 0) AS craver_order_count
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.status = "COMPLETED" 
    AND payment_details.location_id = square_locations.id 
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
  GROUP BY 
    square_locations.id,
    square_merchant_id,
    square_merchant_name
),
temp_active_merchants AS (
  SELECT 
    companies.name, 
    companies.id,
    companies.is_disabled,
    COALESCE(payment_square.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id     
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.payment_square` AS payment_square 
    ON companies.payment_credentials_id = payment_square.id
  WHERE 
    companies.payment_integration = "SQUARE" 
    AND companies.name NOT LIKE '%CHURN%'
    AND companies.is_disabled = 0
  GROUP BY 
    companies.name, 
    companies.id,
    companies.is_disabled,
    square_merchant_id
),
temp_merchant_sales_by_period AS (
  SELECT 
    square_locations.merchant_id AS merchant_id,
    square_merchant_ids.craver_merchant_name AS business_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.created_at)) AS period,
    SUM(payment_details.total_money.amount) AS total_order,
    SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS craver_total,
    SUM(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS non_craver_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS craver_avg_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS non_craver_avg_total,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS non_craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_customer_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_customer_count,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_unique_customers,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_unique_customers,
    (SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) / NULLIF(SUM(payment_details.total_money.amount), 0)) AS northstar_per_merchant
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.location_id = square_locations.id 
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON square_locations.merchant_id = square_merchant_ids.merchant_id      
  INNER JOIN temp_craver_order_count_by_square_location AS temp_craver_order_count_by_square_location
    ON temp_craver_order_count_by_square_location.square_location_id = payment_details.location_id
  WHERE 
    payment_details.status = "COMPLETED" 
    AND temp_craver_order_count_by_square_location.craver_order_count > 0       
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
    AND EXISTS (
      SELECT 1 
      FROM merchant_activity_and_churn_status
      WHERE merchant_activity_and_churn_status.square_merchant_id = square_merchant_ids.merchant_id 
      AND (DATE(payment_details.created_at) >= merchant_activity_and_churn_status.first_order
           AND (DATE(payment_details.created_at) <= merchant_activity_and_churn_status.latest_order 
           OR merchant_activity_and_churn_status.is_churned = FALSE))
    )
  GROUP BY 
    period,
    business_name,
    merchant_id
),
toast_merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name AS name, 
    companies.id AS company_id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  WHERE
    companies.payment_integration = "TOAST"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, companies.is_disabled
),
toast_merchant_orders_by_period AS (
  SELECT 
    SUM(payment_details.checks_total_order_sum) AS total_orders_sum,
    COALESCE(CAST(craver_location_id AS INT), CAST(craver_location_id__it AS INT)) AS craver_location_id_resolved,
    craver_merchant_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.estimatedfulfillmentdate)) AS period
  FROM `craver-capstone-2024-01.merchantdata_20240417.toast_orders`
  WHERE         
    DATE(payment_details.estimatedfulfillmentdate) >= "2023-07-01" 
    AND DATE(payment_details.estimatedfulfillmentdate) <= "2024-03-31"
  GROUP BY
    craver_location_id_resolved,
    craver_merchant_name,
    period
),
toast_merchant_locations AS (
  SELECT 
    locations.id AS id,
    locations.name AS name
  FROM `craver-capstone-2024-01.prod_cafe_20240417.locations` AS locations
  WHERE CAST(locations.is_deleted AS BOOL) = FALSE 
),
toast_merchant_craver_orders_by_period AS (
  SELECT 
    toast_merchant_locations.id as location_id,
    toast_merchant_locations.name as location_name,
    sum(orders.calculated_total) as total_orders_sum,
    FORMAT_DATE("%Y-%m", DATE(orders.created_at)) as period
    FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` as orders
    INNER JOIN toast_merchant_locations as toast_merchant_locations
      ON
        toast_merchant_locations.id = orders.location_fk
    WHERE
      orders.orderStatus = "COMPLETED" 
      AND DATE(orders.created_at) >= "2023-07-01"
      AND DATE(orders.created_at) <= "2024-03-31"
      AND EXISTS 
            (select * from toast_merchant_activity_and_churn_status
              where
                toast_merchant_activity_and_churn_status.company_id = orders.company_ord_fk AND
                (DATE (orders.created_at) >= toast_merchant_activity_and_churn_status.first_order
                AND (DATE (orders.created_at) <= toast_merchant_activity_and_churn_status.latest_order 
                OR toast_merchant_activity_and_churn_status.is_churned = FALSE)))      
    GROUP BY
      location_id,
      location_name,
      period
),
toast_merchant_northstar_by_period_and_location AS (
  SELECT
  COALESCE(toast_merchant_craver_orders_by_period.total_orders_sum/NULLIF(toast_merchant_orders_by_period.total_orders_sum, 0), 0)  as northstar,
  toast_merchant_craver_orders_by_period.total_orders_sum as total_craver_orders_sum,
  toast_merchant_orders_by_period.total_orders_sum as total_orders_sum,
  craver_location_id_resolved,
  craver_merchant_name,
  toast_merchant_craver_orders_by_period.location_name, 
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
),
toast_merchant_northstar_by_period AS (
SELECT
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar,
  craver_merchant_name,
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
toast_merchant_craver_noncraver_orders_by_period AS (
SELECT
  craver_merchant_name as merchant_name,
  toast_merchant_orders_by_period.period as period,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum), 0) as craver_total,
  COALESCE(sum(toast_merchant_orders_by_period.total_orders_sum), 0) as total,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar_per_merchant
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
all_pos_merchant_sales_by_period AS (
   SELECT period, 
    business_name as merchant_name, 
    northstar_per_merchant, 
    craver_total/100 as craver_total, 
    non_craver_total/100 as non_craver_total, 
    total_order/100 as total_order FROM temp_merchant_sales_by_period
    WHERE
      temp_merchant_sales_by_period.northstar_per_merchant <= 1
   UNION ALL
   SELECT period, 
    merchant_name, 
    northstar_per_merchant, 
    craver_total, 
    (total-craver_total) as non_craver_total, 
    (total) as total_order 
   FROM toast_merchant_craver_noncraver_orders_by_period
   WHERE 
      toast_merchant_craver_noncraver_orders_by_period.northstar_per_merchant <= 1
),
upper_quartile_merchants AS (
  SELECT
    merchant_name,
    AVG(northstar_per_merchant) AS avg_northstar_per_merchant,
    SUM(craver_total) AS total_craver_revenue,
    SUM(total_order) AS total_revenue,
    SUM(craver_total) / SUM(total_order) AS overall_northstar_per_merchant
  FROM 
    all_pos_merchant_sales_by_period
  GROUP BY 
    merchant_name
  QUALIFY 
    NTILE(4) OVER (ORDER BY total_revenue DESC) = 1
),
anonymized_merchants AS (
  SELECT
    merchant_name,
    CONCAT('Merchant ', ROW_NUMBER() OVER (ORDER BY overall_northstar_per_merchant DESC)) AS anonymized_merchant
  FROM 
    upper_quartile_merchants
)
SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(craver_total) AS total_revenue,
  'Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant DESC
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant

UNION ALL

SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(non_craver_total) AS total_revenue,
  'Non-Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant DESC
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant;

  
-- Unsuccessful merchants (Craver revenue vs Non-Craver revenue)
WITH square_merchant_ids AS (
  SELECT 
    merchant_id,
    craver_merchant_name,
    COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) AS craver_merchant_square_id_resolved
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_locations`
  WHERE COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) IS NOT NULL
  GROUP BY 
    merchant_id,
    craver_merchant_name,
    craver_merchant_square_id_resolved
),
merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name, 
    companies.id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned,
    COALESCE(square_merchant_ids.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON companies.payment_credentials_id = square_merchant_ids.craver_merchant_square_id_resolved
  WHERE
    companies.payment_integration = "SQUARE"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, square_merchant_id, companies.is_disabled
),
temp_craver_order_count_by_square_location AS (
  SELECT 
    square_locations.id AS square_location_id,
    square_locations.merchant_id AS square_merchant_id,
    square_locations.business_name AS square_merchant_name,
    COALESCE(COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.id) ELSE NULL END), 0) AS craver_order_count
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.status = "COMPLETED" 
    AND payment_details.location_id = square_locations.id 
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
  GROUP BY 
    square_locations.id,
    square_merchant_id,
    square_merchant_name
),
temp_active_merchants AS (
  SELECT 
    companies.name, 
    companies.id,
    companies.is_disabled,
    COALESCE(payment_square.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id     
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.payment_square` AS payment_square 
    ON companies.payment_credentials_id = payment_square.id
  WHERE 
    companies.payment_integration = "SQUARE" 
    AND companies.name NOT LIKE '%CHURN%'
    AND companies.is_disabled = 0
  GROUP BY 
    companies.name, 
    companies.id,
    companies.is_disabled,
    square_merchant_id
),
temp_merchant_sales_by_period AS (
  SELECT 
    square_locations.merchant_id AS merchant_id,
    square_merchant_ids.craver_merchant_name AS business_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.created_at)) AS period,
    SUM(payment_details.total_money.amount) AS total_order,
    SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS craver_total,
    SUM(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS non_craver_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS craver_avg_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS non_craver_avg_total,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS non_craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_customer_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_customer_count,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_unique_customers,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_unique_customers,
    (SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) / NULLIF(SUM(payment_details.total_money.amount), 0)) AS northstar_per_merchant
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.location_id = square_locations.id 
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON square_locations.merchant_id = square_merchant_ids.merchant_id      
  INNER JOIN temp_craver_order_count_by_square_location AS temp_craver_order_count_by_square_location
    ON temp_craver_order_count_by_square_location.square_location_id = payment_details.location_id
  WHERE 
    payment_details.status = "COMPLETED" 
    AND temp_craver_order_count_by_square_location.craver_order_count > 0       
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
    AND EXISTS (
      SELECT 1 
      FROM merchant_activity_and_churn_status
      WHERE merchant_activity_and_churn_status.square_merchant_id = square_merchant_ids.merchant_id 
      AND (DATE(payment_details.created_at) >= merchant_activity_and_churn_status.first_order
           AND (DATE(payment_details.created_at) <= merchant_activity_and_churn_status.latest_order 
           OR merchant_activity_and_churn_status.is_churned = FALSE))
    )
  GROUP BY 
    period,
    business_name,
    merchant_id
),
toast_merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name AS name, 
    companies.id AS company_id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  WHERE
    companies.payment_integration = "TOAST"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, companies.is_disabled
),
toast_merchant_orders_by_period AS (
  SELECT 
    SUM(payment_details.checks_total_order_sum) AS total_orders_sum,
    COALESCE(CAST(craver_location_id AS INT), CAST(craver_location_id__it AS INT)) AS craver_location_id_resolved,
    craver_merchant_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.estimatedfulfillmentdate)) AS period
  FROM `craver-capstone-2024-01.merchantdata_20240417.toast_orders`
  WHERE         
    DATE(payment_details.estimatedfulfillmentdate) >= "2023-07-01" 
    AND DATE(payment_details.estimatedfulfillmentdate) <= "2024-03-31"
  GROUP BY
    craver_location_id_resolved,
    craver_merchant_name,
    period
),
toast_merchant_locations AS (
  SELECT 
    locations.id AS id,
    locations.name AS name
  FROM `craver-capstone-2024-01.prod_cafe_20240417.locations` AS locations
  WHERE CAST(locations.is_deleted AS BOOL) = FALSE 
),
toast_merchant_craver_orders_by_period AS (
  SELECT 
    toast_merchant_locations.id as location_id,
    toast_merchant_locations.name as location_name,
    sum(orders.calculated_total) as total_orders_sum,
    FORMAT_DATE("%Y-%m", DATE(orders.created_at)) as period
    FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` as orders
    INNER JOIN toast_merchant_locations as toast_merchant_locations
      ON
        toast_merchant_locations.id = orders.location_fk
    WHERE
      orders.orderStatus = "COMPLETED" 
      AND DATE(orders.created_at) >= "2023-07-01"
      AND DATE(orders.created_at) <= "2024-03-31"
      AND EXISTS 
            (select * from toast_merchant_activity_and_churn_status
              where
                toast_merchant_activity_and_churn_status.company_id = orders.company_ord_fk AND
                (DATE (orders.created_at) >= toast_merchant_activity_and_churn_status.first_order
                AND (DATE (orders.created_at) <= toast_merchant_activity_and_churn_status.latest_order 
                OR toast_merchant_activity_and_churn_status.is_churned = FALSE)))      
    GROUP BY
      location_id,
      location_name,
      period
),
toast_merchant_northstar_by_period_and_location AS (
  SELECT
  COALESCE(toast_merchant_craver_orders_by_period.total_orders_sum/NULLIF(toast_merchant_orders_by_period.total_orders_sum, 0), 0)  as northstar,
  toast_merchant_craver_orders_by_period.total_orders_sum as total_craver_orders_sum,
  toast_merchant_orders_by_period.total_orders_sum as total_orders_sum,
  craver_location_id_resolved,
  craver_merchant_name,
  toast_merchant_craver_orders_by_period.location_name, 
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
),
toast_merchant_northstar_by_period AS (
SELECT
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar,
  craver_merchant_name,
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
toast_merchant_craver_noncraver_orders_by_period AS (
SELECT
  craver_merchant_name as merchant_name,
  toast_merchant_orders_by_period.period as period,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum), 0) as craver_total,
  COALESCE(sum(toast_merchant_orders_by_period.total_orders_sum), 0) as total,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar_per_merchant
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
all_pos_merchant_sales_by_period AS (
   SELECT period, 
    business_name as merchant_name, 
    northstar_per_merchant, 
    craver_total/100 as craver_total, 
    non_craver_total/100 as non_craver_total, 
    total_order/100 as total_order FROM temp_merchant_sales_by_period
    WHERE
      temp_merchant_sales_by_period.northstar_per_merchant <= 1
   UNION ALL
   SELECT period, 
    merchant_name, 
    northstar_per_merchant, 
    craver_total, 
    (total-craver_total) as non_craver_total, 
    (total) as total_order 
   FROM toast_merchant_craver_noncraver_orders_by_period
   WHERE 
      toast_merchant_craver_noncraver_orders_by_period.northstar_per_merchant <= 1
),
upper_quartile_merchants AS (
  SELECT
    merchant_name,
    AVG(northstar_per_merchant) AS avg_northstar_per_merchant,
    SUM(craver_total) AS total_craver_revenue,
    SUM(total_order) AS total_revenue,
    SUM(craver_total) / SUM(total_order) AS overall_northstar_per_merchant
  FROM 
    all_pos_merchant_sales_by_period
  GROUP BY 
    merchant_name
  QUALIFY 
    NTILE(4) OVER (ORDER BY total_revenue DESC) = 1
),
anonymized_merchants AS (
  SELECT
    merchant_name,
    CONCAT('Merchant ', ROW_NUMBER() OVER (ORDER BY overall_northstar_per_merchant DESC)) AS anonymized_merchant
  FROM 
    upper_quartile_merchants
)
SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(craver_total) AS total_revenue,
  'Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant

UNION ALL

SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(non_craver_total) AS total_revenue,
  'Non-Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant;
  
  
-- Successful merchants (First time vs Repeat customers)
WITH square_merchant_ids AS (
  SELECT 
    merchant_id,
    craver_merchant_name,
    COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) AS craver_merchant_square_id_resolved
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_locations`
  WHERE COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) IS NOT NULL
  GROUP BY 
    merchant_id,
    craver_merchant_name,
    craver_merchant_square_id_resolved
),
merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name, 
    companies.id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned,
    COALESCE(square_merchant_ids.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON companies.payment_credentials_id = square_merchant_ids.craver_merchant_square_id_resolved
  WHERE
    companies.payment_integration = "SQUARE"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, square_merchant_id, companies.is_disabled
),
temp_craver_order_count_by_square_location AS (
  SELECT 
    square_locations.id AS square_location_id,
    square_locations.merchant_id AS square_merchant_id,
    square_locations.business_name AS square_merchant_name,
    COALESCE(COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.id) ELSE NULL END), 0) AS craver_order_count
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.status = "COMPLETED" 
    AND payment_details.location_id = square_locations.id 
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
  GROUP BY 
    square_locations.id,
    square_merchant_id,
    square_merchant_name
),
temp_active_merchants AS (
  SELECT 
    companies.name, 
    companies.id,
    companies.is_disabled,
    COALESCE(payment_square.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id     
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.payment_square` AS payment_square 
    ON companies.payment_credentials_id = payment_square.id
  WHERE 
    companies.payment_integration = "SQUARE" 
    AND companies.name NOT LIKE '%CHURN%'
    AND companies.is_disabled = 0
  GROUP BY 
    companies.name, 
    companies.id,
    companies.is_disabled,
    square_merchant_id
),
temp_merchant_sales_by_period AS (
  SELECT 
    square_locations.merchant_id AS merchant_id,
    square_merchant_ids.craver_merchant_name AS business_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.created_at)) AS period,
    SUM(payment_details.total_money.amount) AS total_order,
    SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS craver_total,
    SUM(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS non_craver_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS craver_avg_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS non_craver_avg_total,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS non_craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_customer_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_customer_count,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_unique_customers,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_unique_customers,
    (SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) / NULLIF(SUM(payment_details.total_money.amount), 0)) AS northstar_per_merchant
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.location_id = square_locations.id 
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON square_locations.merchant_id = square_merchant_ids.merchant_id      
  INNER JOIN temp_craver_order_count_by_square_location AS temp_craver_order_count_by_square_location
    ON temp_craver_order_count_by_square_location.square_location_id = payment_details.location_id
  WHERE 
    payment_details.status = "COMPLETED" 
    AND temp_craver_order_count_by_square_location.craver_order_count > 0       
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
    AND EXISTS (
      SELECT 1 
      FROM merchant_activity_and_churn_status
      WHERE merchant_activity_and_churn_status.square_merchant_id = square_merchant_ids.merchant_id 
      AND (DATE(payment_details.created_at) >= merchant_activity_and_churn_status.first_order
           AND (DATE(payment_details.created_at) <= merchant_activity_and_churn_status.latest_order 
           OR merchant_activity_and_churn_status.is_churned = FALSE))
    )
  GROUP BY 
    period,
    business_name,
    merchant_id
),
toast_merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name AS name, 
    companies.id AS company_id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  WHERE
    companies.payment_integration = "TOAST"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, companies.is_disabled
),
toast_merchant_orders_by_period AS (
  SELECT 
    SUM(payment_details.checks_total_order_sum) AS total_orders_sum,
    COALESCE(CAST(craver_location_id AS INT), CAST(craver_location_id__it AS INT)) AS craver_location_id_resolved,
    craver_merchant_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.estimatedfulfillmentdate)) AS period
  FROM `craver-capstone-2024-01.merchantdata_20240417.toast_orders`
  WHERE         
    DATE(payment_details.estimatedfulfillmentdate) >= "2023-07-01" 
    AND DATE(payment_details.estimatedfulfillmentdate) <= "2024-03-31"
  GROUP BY
    craver_location_id_resolved,
    craver_merchant_name,
    period
),
toast_merchant_locations AS (
  SELECT 
    locations.id AS id,
    locations.name AS name
  FROM `craver-capstone-2024-01.prod_cafe_20240417.locations` AS locations
  WHERE CAST(locations.is_deleted AS BOOL) = FALSE 
),
toast_merchant_craver_orders_by_period AS (
  SELECT 
    toast_merchant_locations.id as location_id,
    toast_merchant_locations.name as location_name,
    sum(orders.calculated_total) as total_orders_sum,
    FORMAT_DATE("%Y-%m", DATE(orders.created_at)) as period
    FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` as orders
    INNER JOIN toast_merchant_locations as toast_merchant_locations
      ON
        toast_merchant_locations.id = orders.location_fk
    WHERE
      orders.orderStatus = "COMPLETED" 
      AND DATE(orders.created_at) >= "2023-07-01"
      AND DATE(orders.created_at) <= "2024-03-31"
      AND EXISTS 
            (select * from toast_merchant_activity_and_churn_status
              where
                toast_merchant_activity_and_churn_status.company_id = orders.company_ord_fk AND
                (DATE (orders.created_at) >= toast_merchant_activity_and_churn_status.first_order
                AND (DATE (orders.created_at) <= toast_merchant_activity_and_churn_status.latest_order 
                OR toast_merchant_activity_and_churn_status.is_churned = FALSE)))      
    GROUP BY
      location_id,
      location_name,
      period
),
toast_merchant_northstar_by_period_and_location AS (
  SELECT
  COALESCE(toast_merchant_craver_orders_by_period.total_orders_sum/NULLIF(toast_merchant_orders_by_period.total_orders_sum, 0), 0)  as northstar,
  toast_merchant_craver_orders_by_period.total_orders_sum as total_craver_orders_sum,
  toast_merchant_orders_by_period.total_orders_sum as total_orders_sum,
  craver_location_id_resolved,
  craver_merchant_name,
  toast_merchant_craver_orders_by_period.location_name, 
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
),
toast_merchant_northstar_by_period AS (
SELECT
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar,
  craver_merchant_name,
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
toast_merchant_craver_noncraver_orders_by_period AS (
SELECT
  craver_merchant_name as merchant_name,
  toast_merchant_orders_by_period.period as period,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum), 0) as craver_total,
  COALESCE(sum(toast_merchant_orders_by_period.total_orders_sum), 0) as total,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar_per_merchant
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
all_pos_merchant_sales_by_period AS (
   SELECT period, 
    business_name as merchant_name, 
    northstar_per_merchant, 
    craver_total/100 as craver_total, 
    non_craver_total/100 as non_craver_total, 
    total_order/100 as total_order FROM temp_merchant_sales_by_period
    WHERE
      temp_merchant_sales_by_period.northstar_per_merchant <= 1
   UNION ALL
   SELECT period, 
    merchant_name, 
    northstar_per_merchant, 
    craver_total, 
    (total-craver_total) as non_craver_total, 
    (total) as total_order 
   FROM toast_merchant_craver_noncraver_orders_by_period
   WHERE 
      toast_merchant_craver_noncraver_orders_by_period.northstar_per_merchant <= 1
),
upper_quartile_merchants AS (
  SELECT
    merchant_name,
    AVG(northstar_per_merchant) AS avg_northstar_per_merchant,
    SUM(craver_total) AS total_craver_revenue,
    SUM(total_order) AS total_revenue,
    SUM(craver_total) / SUM(total_order) AS overall_northstar_per_merchant
  FROM 
    all_pos_merchant_sales_by_period
  GROUP BY 
    merchant_name
  QUALIFY 
    NTILE(4) OVER (ORDER BY total_revenue DESC) = 1
),
anonymized_merchants AS (
  SELECT
    merchant_name,
    CONCAT('Merchant ', ROW_NUMBER() OVER (ORDER BY overall_northstar_per_merchant DESC)) AS anonymized_merchant
  FROM 
    upper_quartile_merchants
),
successful_merchants AS (
SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(craver_total) AS total_revenue,
  'Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant DESC
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant

UNION ALL

SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(non_craver_total) AS total_revenue,
  'Non-Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant DESC
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant
),
customer_orders AS (
    SELECT
        o.user_id_fk AS user_id,
        o.company_ord_fk AS company_id,
        c.name AS merchant_name,
        o.created_at,
        o.orderStatus,
        COALESCE(o.calculated_total, 0) AS order_total,
        COUNT(o.id) OVER (PARTITION BY o.user_id_fk, o.company_ord_fk) AS order_count
    FROM
        `prod_cafe_20240417.orders` o
    JOIN `prod_cafe_20240417.companies` c ON o.company_ord_fk = c.id
    WHERE
        c.name in (select distinct merchant_name from successful_merchants)
        AND LOWER(o.orderStatus) = 'completed'
        AND o.created_at IS NOT NULL
),
first_last_orders AS (
    SELECT
        user_id,
        merchant_name,
        MIN(created_at) AS first_order_date,
        MAX(created_at) AS last_order_date,
        SUM(order_total) AS total_revenue,
        MAX(order_count) AS total_orders
    FROM
        customer_orders
    GROUP BY
        user_id, merchant_name
),
repeat_customers AS (
    SELECT
        merchant_name,
        COUNT(user_id) AS repeat_customer_count,
        AVG(total_orders) AS avg_orders_per_repeat_customer,
        SUM(total_revenue) AS total_revenue_from_repeat_customers
    FROM
        first_last_orders
    WHERE
        total_orders > 1
    GROUP BY
        merchant_name
),
first_time_customers AS (
    SELECT
        merchant_name,
        COUNT(user_id) AS first_time_customer_count,
        SUM(total_revenue) AS total_revenue_from_first_time_customers
    FROM
        first_last_orders
    WHERE
        total_orders = 1
    GROUP BY
        merchant_name
)
SELECT
    rc.merchant_name,
	am.anonymized_merchant,
    rc.repeat_customer_count,
    ft.first_time_customer_count,
    rc.avg_orders_per_repeat_customer,
    rc.total_revenue_from_repeat_customers,
    ft.total_revenue_from_first_time_customers,
    ROUND(rc.total_revenue_from_repeat_customers / (rc.total_revenue_from_repeat_customers + ft.total_revenue_from_first_time_customers) * 100, 2) AS repeat_customer_revenue_percentage,
    ROUND(ft.total_revenue_from_first_time_customers / (rc.total_revenue_from_repeat_customers + ft.total_revenue_from_first_time_customers) * 100, 2) AS first_time_customer_revenue_percentage
FROM
    repeat_customers rc
JOIN
    first_time_customers ft ON rc.merchant_name = ft.merchant_name
JOIN anonymized_merchants am ON rc.merchant_name = am.merchant_name
ORDER BY
    rc.merchant_name;


-- Unsuccessful merchants (First time vs Repeat customers)
WITH square_merchant_ids AS (
  SELECT 
    merchant_id,
    craver_merchant_name,
    COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) AS craver_merchant_square_id_resolved
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_locations`
  WHERE COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) IS NOT NULL
  GROUP BY 
    merchant_id,
    craver_merchant_name,
    craver_merchant_square_id_resolved
),
merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name, 
    companies.id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned,
    COALESCE(square_merchant_ids.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON companies.payment_credentials_id = square_merchant_ids.craver_merchant_square_id_resolved
  WHERE
    companies.payment_integration = "SQUARE"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, square_merchant_id, companies.is_disabled
),
temp_craver_order_count_by_square_location AS (
  SELECT 
    square_locations.id AS square_location_id,
    square_locations.merchant_id AS square_merchant_id,
    square_locations.business_name AS square_merchant_name,
    COALESCE(COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.id) ELSE NULL END), 0) AS craver_order_count
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.status = "COMPLETED" 
    AND payment_details.location_id = square_locations.id 
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
  GROUP BY 
    square_locations.id,
    square_merchant_id,
    square_merchant_name
),
temp_active_merchants AS (
  SELECT 
    companies.name, 
    companies.id,
    companies.is_disabled,
    COALESCE(payment_square.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id     
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.payment_square` AS payment_square 
    ON companies.payment_credentials_id = payment_square.id
  WHERE 
    companies.payment_integration = "SQUARE" 
    AND companies.name NOT LIKE '%CHURN%'
    AND companies.is_disabled = 0
  GROUP BY 
    companies.name, 
    companies.id,
    companies.is_disabled,
    square_merchant_id
),
temp_merchant_sales_by_period AS (
  SELECT 
    square_locations.merchant_id AS merchant_id,
    square_merchant_ids.craver_merchant_name AS business_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.created_at)) AS period,
    SUM(payment_details.total_money.amount) AS total_order,
    SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS craver_total,
    SUM(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS non_craver_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS craver_avg_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS non_craver_avg_total,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS non_craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_customer_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_customer_count,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_unique_customers,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_unique_customers,
    (SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) / NULLIF(SUM(payment_details.total_money.amount), 0)) AS northstar_per_merchant
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.location_id = square_locations.id 
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON square_locations.merchant_id = square_merchant_ids.merchant_id      
  INNER JOIN temp_craver_order_count_by_square_location AS temp_craver_order_count_by_square_location
    ON temp_craver_order_count_by_square_location.square_location_id = payment_details.location_id
  WHERE 
    payment_details.status = "COMPLETED" 
    AND temp_craver_order_count_by_square_location.craver_order_count > 0       
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
    AND EXISTS (
      SELECT 1 
      FROM merchant_activity_and_churn_status
      WHERE merchant_activity_and_churn_status.square_merchant_id = square_merchant_ids.merchant_id 
      AND (DATE(payment_details.created_at) >= merchant_activity_and_churn_status.first_order
           AND (DATE(payment_details.created_at) <= merchant_activity_and_churn_status.latest_order 
           OR merchant_activity_and_churn_status.is_churned = FALSE))
    )
  GROUP BY 
    period,
    business_name,
    merchant_id
),
toast_merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name AS name, 
    companies.id AS company_id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  WHERE
    companies.payment_integration = "TOAST"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, companies.is_disabled
),
toast_merchant_orders_by_period AS (
  SELECT 
    SUM(payment_details.checks_total_order_sum) AS total_orders_sum,
    COALESCE(CAST(craver_location_id AS INT), CAST(craver_location_id__it AS INT)) AS craver_location_id_resolved,
    craver_merchant_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.estimatedfulfillmentdate)) AS period
  FROM `craver-capstone-2024-01.merchantdata_20240417.toast_orders`
  WHERE         
    DATE(payment_details.estimatedfulfillmentdate) >= "2023-07-01" 
    AND DATE(payment_details.estimatedfulfillmentdate) <= "2024-03-31"
  GROUP BY
    craver_location_id_resolved,
    craver_merchant_name,
    period
),
toast_merchant_locations AS (
  SELECT 
    locations.id AS id,
    locations.name AS name
  FROM `craver-capstone-2024-01.prod_cafe_20240417.locations` AS locations
  WHERE CAST(locations.is_deleted AS BOOL) = FALSE 
),
toast_merchant_craver_orders_by_period AS (
  SELECT 
    toast_merchant_locations.id as location_id,
    toast_merchant_locations.name as location_name,
    sum(orders.calculated_total) as total_orders_sum,
    FORMAT_DATE("%Y-%m", DATE(orders.created_at)) as period
    FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` as orders
    INNER JOIN toast_merchant_locations as toast_merchant_locations
      ON
        toast_merchant_locations.id = orders.location_fk
    WHERE
      orders.orderStatus = "COMPLETED" 
      AND DATE(orders.created_at) >= "2023-07-01"
      AND DATE(orders.created_at) <= "2024-03-31"
      AND EXISTS 
            (select * from toast_merchant_activity_and_churn_status
              where
                toast_merchant_activity_and_churn_status.company_id = orders.company_ord_fk AND
                (DATE (orders.created_at) >= toast_merchant_activity_and_churn_status.first_order
                AND (DATE (orders.created_at) <= toast_merchant_activity_and_churn_status.latest_order 
                OR toast_merchant_activity_and_churn_status.is_churned = FALSE)))      
    GROUP BY
      location_id,
      location_name,
      period
),
toast_merchant_northstar_by_period_and_location AS (
  SELECT
  COALESCE(toast_merchant_craver_orders_by_period.total_orders_sum/NULLIF(toast_merchant_orders_by_period.total_orders_sum, 0), 0)  as northstar,
  toast_merchant_craver_orders_by_period.total_orders_sum as total_craver_orders_sum,
  toast_merchant_orders_by_period.total_orders_sum as total_orders_sum,
  craver_location_id_resolved,
  craver_merchant_name,
  toast_merchant_craver_orders_by_period.location_name, 
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
),
toast_merchant_northstar_by_period AS (
SELECT
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar,
  craver_merchant_name,
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
toast_merchant_craver_noncraver_orders_by_period AS (
SELECT
  craver_merchant_name as merchant_name,
  toast_merchant_orders_by_period.period as period,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum), 0) as craver_total,
  COALESCE(sum(toast_merchant_orders_by_period.total_orders_sum), 0) as total,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar_per_merchant
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
all_pos_merchant_sales_by_period AS (
   SELECT period, 
    business_name as merchant_name, 
    northstar_per_merchant, 
    craver_total/100 as craver_total, 
    non_craver_total/100 as non_craver_total, 
    total_order/100 as total_order FROM temp_merchant_sales_by_period
    WHERE
      temp_merchant_sales_by_period.northstar_per_merchant <= 1
   UNION ALL
   SELECT period, 
    merchant_name, 
    northstar_per_merchant, 
    craver_total, 
    (total-craver_total) as non_craver_total, 
    (total) as total_order 
   FROM toast_merchant_craver_noncraver_orders_by_period
   WHERE 
      toast_merchant_craver_noncraver_orders_by_period.northstar_per_merchant <= 1
),
upper_quartile_merchants AS (
  SELECT
    merchant_name,
    AVG(northstar_per_merchant) AS avg_northstar_per_merchant,
    SUM(craver_total) AS total_craver_revenue,
    SUM(total_order) AS total_revenue,
    SUM(craver_total) / SUM(total_order) AS overall_northstar_per_merchant
  FROM 
    all_pos_merchant_sales_by_period
  GROUP BY 
    merchant_name
  QUALIFY 
    NTILE(4) OVER (ORDER BY total_revenue DESC) = 1
),
anonymized_merchants AS (
  SELECT
    merchant_name,
    CONCAT('Merchant ', ROW_NUMBER() OVER (ORDER BY overall_northstar_per_merchant DESC)) AS anonymized_merchant
  FROM 
    upper_quartile_merchants
),
unsuccessful_merchants AS (
SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(craver_total) AS total_revenue,
  'Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant

UNION ALL

SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(non_craver_total) AS total_revenue,
  'Non-Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant
    LIMIT 
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant
),
customer_orders AS (
    SELECT
        o.user_id_fk AS user_id,
        o.company_ord_fk AS company_id,
        c.name AS merchant_name,
        o.created_at,
        o.orderStatus,
        COALESCE(o.calculated_total, 0) AS order_total,
        COUNT(o.id) OVER (PARTITION BY o.user_id_fk, o.company_ord_fk) AS order_count
    FROM
        `prod_cafe_20240417.orders` o
    JOIN `prod_cafe_20240417.companies` c ON o.company_ord_fk = c.id
    WHERE
        c.name in (select distinct merchant_name from unsuccessful_merchants)
        AND LOWER(o.orderStatus) = 'completed'
        AND o.created_at IS NOT NULL
),
first_last_orders AS (
    SELECT
        user_id,
        merchant_name,
        MIN(created_at) AS first_order_date,
        MAX(created_at) AS last_order_date,
        SUM(order_total) AS total_revenue,
        MAX(order_count) AS total_orders
    FROM
        customer_orders
    GROUP BY
        user_id, merchant_name
),
repeat_customers AS (
    SELECT
        merchant_name,
        COUNT(user_id) AS repeat_customer_count,
        AVG(total_orders) AS avg_orders_per_repeat_customer,
        SUM(total_revenue) AS total_revenue_from_repeat_customers
    FROM
        first_last_orders
    WHERE
        total_orders > 1
    GROUP BY
        merchant_name
),
first_time_customers AS (
    SELECT
        merchant_name,
        COUNT(user_id) AS first_time_customer_count,
        SUM(total_revenue) AS total_revenue_from_first_time_customers
    FROM
        first_last_orders
    WHERE
        total_orders = 1
    GROUP BY
        merchant_name
)
SELECT
    rc.merchant_name,
	am.anonymized_merchant,
    rc.repeat_customer_count,
    ft.first_time_customer_count,
    rc.avg_orders_per_repeat_customer,
    rc.total_revenue_from_repeat_customers,
    ft.total_revenue_from_first_time_customers,
    ROUND(rc.total_revenue_from_repeat_customers / (rc.total_revenue_from_repeat_customers + ft.total_revenue_from_first_time_customers) * 100, 2) AS repeat_customer_revenue_percentage,
    ROUND(ft.total_revenue_from_first_time_customers / (rc.total_revenue_from_repeat_customers + ft.total_revenue_from_first_time_customers) * 100, 2) AS first_time_customer_revenue_percentage
FROM
    repeat_customers rc
JOIN
    first_time_customers ft ON rc.merchant_name = ft.merchant_name
JOIN anonymized_merchants am ON rc.merchant_name = am.merchant_name
ORDER BY
    rc.merchant_name;
	
	
-- RFM Analysis - Successful Merchants
WITH square_merchant_ids AS (
  SELECT 
    merchant_id,
    craver_merchant_name,
    COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) AS craver_merchant_square_id_resolved
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_locations`
  WHERE COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) IS NOT NULL
  GROUP BY 
    merchant_id,
    craver_merchant_name,
    craver_merchant_square_id_resolved
),
merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name, 
    companies.id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned,
    COALESCE(square_merchant_ids.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON companies.payment_credentials_id = square_merchant_ids.craver_merchant_square_id_resolved
  WHERE
    companies.payment_integration = "SQUARE"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, square_merchant_id, companies.is_disabled
),
temp_craver_order_count_by_square_location AS (
  SELECT 
    square_locations.id AS square_location_id,
    square_locations.merchant_id AS square_merchant_id,
    square_locations.business_name AS square_merchant_name,
    COALESCE(COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.id) ELSE NULL END), 0) AS craver_order_count
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.status = "COMPLETED" 
    AND payment_details.location_id = square_locations.id 
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
  GROUP BY 
    square_locations.id,
    square_merchant_id,
    square_merchant_name
),
temp_active_merchants AS (
  SELECT 
    companies.name, 
    companies.id,
    companies.is_disabled,
    COALESCE(payment_square.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id     
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.payment_square` AS payment_square 
    ON companies.payment_credentials_id = payment_square.id
  WHERE 
    companies.payment_integration = "SQUARE" 
    AND companies.name NOT LIKE '%CHURN%'
    AND companies.is_disabled = 0
  GROUP BY 
    companies.name, 
    companies.id,
    companies.is_disabled,
    square_merchant_id
),
temp_merchant_sales_by_period AS (
  SELECT 
    square_locations.merchant_id AS merchant_id,
    square_merchant_ids.craver_merchant_name AS business_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.created_at)) AS period,
    SUM(payment_details.total_money.amount) AS total_order,
    SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS craver_total,
    SUM(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS non_craver_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS craver_avg_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS non_craver_avg_total,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS non_craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_customer_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_customer_count,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_unique_customers,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_unique_customers,
    (SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) / NULLIF(SUM(payment_details.total_money.amount), 0)) AS northstar_per_merchant
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.location_id = square_locations.id 
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON square_locations.merchant_id = square_merchant_ids.merchant_id      
  INNER JOIN temp_craver_order_count_by_square_location AS temp_craver_order_count_by_square_location
    ON temp_craver_order_count_by_square_location.square_location_id = payment_details.location_id
  WHERE 
    payment_details.status = "COMPLETED" 
    AND temp_craver_order_count_by_square_location.craver_order_count > 0       
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
    AND EXISTS (
      SELECT 1 
      FROM merchant_activity_and_churn_status
      WHERE merchant_activity_and_churn_status.square_merchant_id = square_merchant_ids.merchant_id 
      AND (DATE(payment_details.created_at) >= merchant_activity_and_churn_status.first_order
           AND (DATE(payment_details.created_at) <= merchant_activity_and_churn_status.latest_order 
           OR merchant_activity_and_churn_status.is_churned = FALSE))
    )
  GROUP BY 
    period,
    business_name,
    merchant_id
),
toast_merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name AS name, 
    companies.id AS company_id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  WHERE
    companies.payment_integration = "TOAST"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, companies.is_disabled
),
toast_merchant_orders_by_period AS (
  SELECT 
    SUM(payment_details.checks_total_order_sum) AS total_orders_sum,
    COALESCE(CAST(craver_location_id AS INT), CAST(craver_location_id__it AS INT)) AS craver_location_id_resolved,
    craver_merchant_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.estimatedfulfillmentdate)) AS period
  FROM `craver-capstone-2024-01.merchantdata_20240417.toast_orders`
  WHERE         
    DATE(payment_details.estimatedfulfillmentdate) >= "2023-07-01" 
    AND DATE(payment_details.estimatedfulfillmentdate) <= "2024-03-31"
  GROUP BY
    craver_location_id_resolved,
    craver_merchant_name,
    period
),
toast_merchant_locations AS (
  SELECT 
    locations.id AS id,
    locations.name AS name
  FROM `craver-capstone-2024-01.prod_cafe_20240417.locations` AS locations
  WHERE CAST(locations.is_deleted AS BOOL) = FALSE 
),
toast_merchant_craver_orders_by_period AS (
  SELECT 
    toast_merchant_locations.id as location_id,
    toast_merchant_locations.name as location_name,
    sum(orders.calculated_total) as total_orders_sum,
    FORMAT_DATE("%Y-%m", DATE(orders.created_at)) as period
    FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` as orders
    INNER JOIN toast_merchant_locations as toast_merchant_locations
      ON
        toast_merchant_locations.id = orders.location_fk
    WHERE
      orders.orderStatus = "COMPLETED" 
      AND DATE(orders.created_at) >= "2023-07-01"
      AND DATE(orders.created_at) <= "2024-03-31"
      AND EXISTS 
            (select * from toast_merchant_activity_and_churn_status
              where
                toast_merchant_activity_and_churn_status.company_id = orders.company_ord_fk AND
                (DATE (orders.created_at) >= toast_merchant_activity_and_churn_status.first_order
                AND (DATE (orders.created_at) <= toast_merchant_activity_and_churn_status.latest_order 
                OR toast_merchant_activity_and_churn_status.is_churned = FALSE)))      
    GROUP BY
      location_id,
      location_name,
      period
),
toast_merchant_northstar_by_period_and_location AS (
  SELECT
  COALESCE(toast_merchant_craver_orders_by_period.total_orders_sum/NULLIF(toast_merchant_orders_by_period.total_orders_sum, 0), 0)  as northstar,
  toast_merchant_craver_orders_by_period.total_orders_sum as total_craver_orders_sum,
  toast_merchant_orders_by_period.total_orders_sum as total_orders_sum,
  craver_location_id_resolved,
  craver_merchant_name,
  toast_merchant_craver_orders_by_period.location_name, 
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
),
toast_merchant_northstar_by_period AS (
SELECT
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar,
  craver_merchant_name,
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
toast_merchant_craver_noncraver_orders_by_period AS (
SELECT
  craver_merchant_name as merchant_name,
  toast_merchant_orders_by_period.period as period,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum), 0) as craver_total,
  COALESCE(sum(toast_merchant_orders_by_period.total_orders_sum), 0) as total,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar_per_merchant
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
all_pos_merchant_sales_by_period AS (
   SELECT period, 
    business_name as merchant_name, 
    northstar_per_merchant, 
    craver_total/100 as craver_total, 
    non_craver_total/100 as non_craver_total, 
    total_order/100 as total_order FROM temp_merchant_sales_by_period
    WHERE
      temp_merchant_sales_by_period.northstar_per_merchant <= 1
   UNION ALL
   SELECT period, 
    merchant_name, 
    northstar_per_merchant, 
    craver_total, 
    (total-craver_total) as non_craver_total, 
    (total) as total_order 
   FROM toast_merchant_craver_noncraver_orders_by_period
   WHERE 
      toast_merchant_craver_noncraver_orders_by_period.northstar_per_merchant <= 1
),
upper_quartile_merchants AS (
  SELECT
    merchant_name,
    AVG(northstar_per_merchant) AS avg_northstar_per_merchant,
    SUM(craver_total) AS total_craver_revenue,
    SUM(total_order) AS total_revenue,
    SUM(craver_total) / SUM(total_order) AS overall_northstar_per_merchant
  FROM 
    all_pos_merchant_sales_by_period
  GROUP BY 
    merchant_name
  QUALIFY 
    NTILE(4) OVER (ORDER BY total_revenue DESC) = 1
),
anonymized_merchants AS (
  SELECT
    merchant_name,
    CONCAT('Merchant ', ROW_NUMBER() OVER (ORDER BY overall_northstar_per_merchant DESC)) AS anonymized_merchant
  FROM 
    upper_quartile_merchants
),
successful_merchants AS (
SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(craver_total) AS total_revenue,
  'Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant DESC
    LIMIT
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant

UNION ALL

SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(non_craver_total) AS total_revenue,
  'Non-Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant DESC
    LIMIT
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant
),
rfm_scores AS (
    SELECT 
        uqm.merchant_name,
  		uqm.overall_northstar_per_merchant,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(o.created_at)), DAY) AS recency,
        MAX(o.created_at) AS last_order_date,
        COUNT(o.id) AS frequency,
        SUM(o.calculated_total) AS monetary
    FROM
		upper_quartile_merchants uqm
	JOIN
		craver-capstone-2024-01.prod_cafe_20240417.companies c on uqm.merchant_name = c.name
	JOIN
        craver-capstone-2024-01.prod_cafe_20240417.orders o ON c.id = o.company_ord_fk
    WHERE
        o.created_at IS NOT NULL
    GROUP BY 
        uqm.merchant_name, uqm.overall_northstar_per_merchant
    HAVING
        SUM(o.calculated_total) != 0
),
quartiles AS (
    SELECT
        merchant_name,
        overall_northstar_per_merchant,
        recency,
        last_order_date,
        frequency,
        monetary,
        NTILE(4) OVER (ORDER BY recency desc, frequency) AS r_quartile,
        NTILE(4) OVER (ORDER BY frequency) AS f_quartile,
        NTILE(4) OVER (ORDER BY overall_northstar_per_merchant) AS m_quartile
    FROM 
        rfm_scores
)
SELECT 
    q.merchant_name,
	am.anonymized_merchant,
	overall_northstar_per_merchant,
    recency,
    frequency,
    monetary,
    r_quartile,
    f_quartile,
    m_quartile,
    CONCAT(CAST(r_quartile AS STRING), CAST(f_quartile AS STRING), CAST(m_quartile AS STRING)) AS rfm_score,
	((0.28287841191067/4) * r_quartile) + ((0.34987593052109184/4) * f_quartile) + ((0.3672456575682382/4) * m_quartile) AS weighted_rfm
FROM 
    quartiles q
JOIN
	anonymized_merchants am ON am.merchant_name = q.merchant_name
WHERE q.merchant_name IN (select merchant_name from successful_merchants)
ORDER BY recency;



-- RFM Analysis - Unsuccessful Merchants
WITH square_merchant_ids AS (
  SELECT 
    merchant_id,
    craver_merchant_name,
    COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) AS craver_merchant_square_id_resolved
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_locations`
  WHERE COALESCE(CAST(craver_merchant_square_id AS INT), CAST(craver_merchant_square_id__it AS INT)) IS NOT NULL
  GROUP BY 
    merchant_id,
    craver_merchant_name,
    craver_merchant_square_id_resolved
),
merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name, 
    companies.id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned,
    COALESCE(square_merchant_ids.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON companies.payment_credentials_id = square_merchant_ids.craver_merchant_square_id_resolved
  WHERE
    companies.payment_integration = "SQUARE"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, square_merchant_id, companies.is_disabled
),
temp_craver_order_count_by_square_location AS (
  SELECT 
    square_locations.id AS square_location_id,
    square_locations.merchant_id AS square_merchant_id,
    square_locations.business_name AS square_merchant_name,
    COALESCE(COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.id) ELSE NULL END), 0) AS craver_order_count
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.status = "COMPLETED" 
    AND payment_details.location_id = square_locations.id 
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
  GROUP BY 
    square_locations.id,
    square_merchant_id,
    square_merchant_name
),
temp_active_merchants AS (
  SELECT 
    companies.name, 
    companies.id,
    companies.is_disabled,
    COALESCE(payment_square.merchant_id, companies.square_merchant_id) AS square_merchant_id
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id     
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.payment_square` AS payment_square 
    ON companies.payment_credentials_id = payment_square.id
  WHERE 
    companies.payment_integration = "SQUARE" 
    AND companies.name NOT LIKE '%CHURN%'
    AND companies.is_disabled = 0
  GROUP BY 
    companies.name, 
    companies.id,
    companies.is_disabled,
    square_merchant_id
),
temp_merchant_sales_by_period AS (
  SELECT 
    square_locations.merchant_id AS merchant_id,
    square_merchant_ids.craver_merchant_name AS business_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.created_at)) AS period,
    SUM(payment_details.total_money.amount) AS total_order,
    SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS craver_total,
    SUM(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) AS non_craver_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS craver_avg_total,
    AVG(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE NULL END) AS non_craver_avg_total,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, NULL) ELSE NULL END) AS non_craver_order_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_customer_count,
    COUNT(CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_customer_count,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS craver_unique_customers,
    COUNT(DISTINCT CASE WHEN COALESCE(payment_details.note, "") NOT LIKE '%Craver%' THEN (payment_details.customer_id) ELSE NULL END) AS non_craver_unique_customers,
    (SUM(CASE WHEN COALESCE(payment_details.note, "") LIKE '%Craver%' THEN COALESCE(payment_details.total_money.amount, 0) ELSE 0 END) / NULLIF(SUM(payment_details.total_money.amount), 0)) AS northstar_per_merchant
  FROM `craver-capstone-2024-01.merchantdata_20240417.square_payments` 
  INNER JOIN `craver-capstone-2024-01.merchantdata_20240417.square_locations` AS square_locations
    ON payment_details.location_id = square_locations.id 
  INNER JOIN square_merchant_ids AS square_merchant_ids
    ON square_locations.merchant_id = square_merchant_ids.merchant_id      
  INNER JOIN temp_craver_order_count_by_square_location AS temp_craver_order_count_by_square_location
    ON temp_craver_order_count_by_square_location.square_location_id = payment_details.location_id
  WHERE 
    payment_details.status = "COMPLETED" 
    AND temp_craver_order_count_by_square_location.craver_order_count > 0       
    AND DATE(payment_details.created_at) >= "2023-07-01" 
    AND DATE(payment_details.created_at) <= "2024-03-31"
    AND EXISTS (
      SELECT 1 
      FROM merchant_activity_and_churn_status
      WHERE merchant_activity_and_churn_status.square_merchant_id = square_merchant_ids.merchant_id 
      AND (DATE(payment_details.created_at) >= merchant_activity_and_churn_status.first_order
           AND (DATE(payment_details.created_at) <= merchant_activity_and_churn_status.latest_order 
           OR merchant_activity_and_churn_status.is_churned = FALSE))
    )
  GROUP BY 
    period,
    business_name,
    merchant_id
),
toast_merchant_activity_and_churn_status AS (
  SELECT 
    MIN(DATE(orders.created_at)) AS first_order,
    MAX(DATE(orders.created_at)) AS latest_order,
    companies.name AS name, 
    companies.id AS company_id,
    (companies.is_disabled = 1 OR companies.name LIKE '%CHURN%') AS is_churned
  FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` AS orders
  INNER JOIN `craver-capstone-2024-01.prod_cafe_20240417.companies` AS companies 
    ON orders.company_ord_fk = companies.id       
  WHERE
    companies.payment_integration = "TOAST"       
    AND orders.orderStatus = "COMPLETED" 
  GROUP BY 
    companies.name, companies.id, companies.is_disabled
),
toast_merchant_orders_by_period AS (
  SELECT 
    SUM(payment_details.checks_total_order_sum) AS total_orders_sum,
    COALESCE(CAST(craver_location_id AS INT), CAST(craver_location_id__it AS INT)) AS craver_location_id_resolved,
    craver_merchant_name,
    FORMAT_DATE("%Y-%m", DATE(payment_details.estimatedfulfillmentdate)) AS period
  FROM `craver-capstone-2024-01.merchantdata_20240417.toast_orders`
  WHERE         
    DATE(payment_details.estimatedfulfillmentdate) >= "2023-07-01" 
    AND DATE(payment_details.estimatedfulfillmentdate) <= "2024-03-31"
  GROUP BY
    craver_location_id_resolved,
    craver_merchant_name,
    period
),
toast_merchant_locations AS (
  SELECT 
    locations.id AS id,
    locations.name AS name
  FROM `craver-capstone-2024-01.prod_cafe_20240417.locations` AS locations
  WHERE CAST(locations.is_deleted AS BOOL) = FALSE 
),
toast_merchant_craver_orders_by_period AS (
  SELECT 
    toast_merchant_locations.id as location_id,
    toast_merchant_locations.name as location_name,
    sum(orders.calculated_total) as total_orders_sum,
    FORMAT_DATE("%Y-%m", DATE(orders.created_at)) as period
    FROM `craver-capstone-2024-01.prod_cafe_20240417.orders` as orders
    INNER JOIN toast_merchant_locations as toast_merchant_locations
      ON
        toast_merchant_locations.id = orders.location_fk
    WHERE
      orders.orderStatus = "COMPLETED" 
      AND DATE(orders.created_at) >= "2023-07-01"
      AND DATE(orders.created_at) <= "2024-03-31"
      AND EXISTS 
            (select * from toast_merchant_activity_and_churn_status
              where
                toast_merchant_activity_and_churn_status.company_id = orders.company_ord_fk AND
                (DATE (orders.created_at) >= toast_merchant_activity_and_churn_status.first_order
                AND (DATE (orders.created_at) <= toast_merchant_activity_and_churn_status.latest_order 
                OR toast_merchant_activity_and_churn_status.is_churned = FALSE)))      
    GROUP BY
      location_id,
      location_name,
      period
),
toast_merchant_northstar_by_period_and_location AS (
  SELECT
  COALESCE(toast_merchant_craver_orders_by_period.total_orders_sum/NULLIF(toast_merchant_orders_by_period.total_orders_sum, 0), 0)  as northstar,
  toast_merchant_craver_orders_by_period.total_orders_sum as total_craver_orders_sum,
  toast_merchant_orders_by_period.total_orders_sum as total_orders_sum,
  craver_location_id_resolved,
  craver_merchant_name,
  toast_merchant_craver_orders_by_period.location_name, 
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
),
toast_merchant_northstar_by_period AS (
SELECT
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar,
  craver_merchant_name,
  toast_merchant_orders_by_period.period as period
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
toast_merchant_craver_noncraver_orders_by_period AS (
SELECT
  craver_merchant_name as merchant_name,
  toast_merchant_orders_by_period.period as period,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum), 0) as craver_total,
  COALESCE(sum(toast_merchant_orders_by_period.total_orders_sum), 0) as total,
  COALESCE(sum(toast_merchant_craver_orders_by_period.total_orders_sum)/NULLIF(sum(toast_merchant_orders_by_period.total_orders_sum), 0), 0) as northstar_per_merchant
  FROM toast_merchant_orders_by_period
  INNER JOIN toast_merchant_craver_orders_by_period
    ON
      toast_merchant_orders_by_period.period = toast_merchant_craver_orders_by_period.period AND
      toast_merchant_orders_by_period.craver_location_id_resolved = toast_merchant_craver_orders_by_period.location_id
  GROUP BY
    period,
    craver_merchant_name
),
all_pos_merchant_sales_by_period AS (
   SELECT period, 
    business_name as merchant_name, 
    northstar_per_merchant, 
    craver_total/100 as craver_total, 
    non_craver_total/100 as non_craver_total, 
    total_order/100 as total_order FROM temp_merchant_sales_by_period
    WHERE
      temp_merchant_sales_by_period.northstar_per_merchant <= 1
   UNION ALL
   SELECT period, 
    merchant_name, 
    northstar_per_merchant, 
    craver_total, 
    (total-craver_total) as non_craver_total, 
    (total) as total_order 
   FROM toast_merchant_craver_noncraver_orders_by_period
   WHERE 
      toast_merchant_craver_noncraver_orders_by_period.northstar_per_merchant <= 1
),
upper_quartile_merchants AS (
  SELECT
    merchant_name,
    AVG(northstar_per_merchant) AS avg_northstar_per_merchant,
    SUM(craver_total) AS total_craver_revenue,
    SUM(total_order) AS total_revenue,
    SUM(craver_total) / SUM(total_order) AS overall_northstar_per_merchant
  FROM 
    all_pos_merchant_sales_by_period
  GROUP BY 
    merchant_name
  QUALIFY 
    NTILE(4) OVER (ORDER BY total_revenue DESC) = 1
),
anonymized_merchants AS (
  SELECT
    merchant_name,
    CONCAT('Merchant ', ROW_NUMBER() OVER (ORDER BY overall_northstar_per_merchant DESC)) AS anonymized_merchant
  FROM 
    upper_quartile_merchants
),
unsuccessful_merchants AS (
SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(craver_total) AS total_revenue,
  'Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant
    LIMIT
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant

UNION ALL

SELECT 
  alpm.merchant_name,
  am.anonymized_merchant,
  SUM(non_craver_total) AS total_revenue,
  'Non-Craver' AS type
FROM 
  all_pos_merchant_sales_by_period alpm
JOIN
  anonymized_merchants am ON alpm.merchant_name = am.merchant_name
WHERE 
  alpm.merchant_name IN (
    SELECT 
      merchant_name 
    FROM 
      upper_quartile_merchants 
    ORDER BY 
      overall_northstar_per_merchant
    LIMIT
      10
  ) 
GROUP BY 
  alpm.merchant_name, am.anonymized_merchant
),
rfm_scores AS (
    SELECT 
        uqm.merchant_name,
  		uqm.overall_northstar_per_merchant,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(o.created_at)), DAY) AS recency,
        MAX(o.created_at) AS last_order_date,
        COUNT(o.id) AS frequency,
        SUM(o.calculated_total) AS monetary
    FROM
		upper_quartile_merchants uqm
	JOIN
		craver-capstone-2024-01.prod_cafe_20240417.companies c on uqm.merchant_name = c.name
	JOIN
        craver-capstone-2024-01.prod_cafe_20240417.orders o ON c.id = o.company_ord_fk
    WHERE
        o.created_at IS NOT NULL
    GROUP BY 
        uqm.merchant_name, uqm.overall_northstar_per_merchant
    HAVING
        SUM(o.calculated_total) != 0
),
quartiles AS (
    SELECT
        merchant_name,
  		overall_northstar_per_merchant,
        recency,
        last_order_date,
        frequency,
        monetary,
        NTILE(4) OVER (ORDER BY recency desc, frequency) AS r_quartile,
        NTILE(4) OVER (ORDER BY frequency) AS f_quartile,
        NTILE(4) OVER (ORDER BY overall_northstar_per_merchant) AS m_quartile
    FROM 
        rfm_scores
)
SELECT 
    q.merchant_name,
	am.anonymized_merchant,
	overall_northstar_per_merchant,
    recency,
    frequency,
    monetary,
    r_quartile,
    f_quartile,
    m_quartile,
    CONCAT(CAST(r_quartile AS STRING), CAST(f_quartile AS STRING), CAST(m_quartile AS STRING)) AS rfm_score,
	((0.28287841191067/4) * r_quartile) + ((0.34987593052109184/4) * f_quartile) + ((0.3672456575682382/4) * m_quartile) AS weighted_rfm
FROM 
    quartiles q
JOIN
	anonymized_merchants am ON am.merchant_name = q.merchant_name
WHERE q.merchant_name IN (select merchant_name from unsuccessful_merchants)
ORDER BY recency;