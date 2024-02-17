


SELECT dp.product_code, dp.product_name, dp.Category, fe.base_price, fe.promo_type
FROM dim_products dp
JOIN fact_events fe ON dp.product_code = fe.product_code
WHERE fe.base_price > 500 AND fe.promo_type LIKE 'BOGOF%'
GROUP BY dp.product_code, dp.product_name, dp.Category, fe.base_price, fe.promo_type;


select count(store_id) as number_of_stores,city from dim_stores  group by city order by count(*) desc;




WITH cte AS (
    SELECT
        campaign_id,
        SUM(
            CASE 
                WHEN promo_type = 'BOGOF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price / 2)
                WHEN promo_type = '50% OFF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price * 0.5)
                WHEN promo_type = '33% OFF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price * 0.67)
                WHEN promo_type = '500 Cashback' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price - 500)
                WHEN promo_type = '25% OFF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price * 0.75)
                ELSE
                    CAST(quantity_sold_after_promo AS bigint) * base_price
            END
        ) AS total_product_revenue_after_campaign,
        SUM(CAST(quantity_sold_before_promo AS bigint) * base_price ) AS total_product_revenue_before_campaign
    FROM
        fact_events
    GROUP BY
        campaign_id, promo_type
)

SELECT
    campaign_id,
    FORMAT(SUM(total_product_revenue_before_campaign), '0,,.00M') AS total_revenue_before_campaign,
    FORMAT(SUM(total_product_revenue_after_campaign), '0,,.00M') AS total_revenue_after_campaign
FROM
    cte
GROUP BY
    campaign_id;


WITH cte AS (
  SELECT
    dp.category,
    (((SUM(fe.quantity_sold_after_promo) - SUM(fe.quantity_sold_before_promo))*100) / SUM(fe.quantity_sold_before_promo))  AS isu_percentage
  FROM
    dim_products dp
  JOIN
    fact_events fe ON dp.product_code = fe.product_code
  WHERE
    fe.campaign_id = 'CAMP_DIW_01'
  GROUP BY
    dp.category
)

SELECT
  category,
  isu_percentage,
  RANK() OVER (ORDER BY isu_percentage DESC) AS isu_rank
FROM
  cte;

with cte as (
    SELECT
        dp.product_name,dp.category,
        SUM(CAST(quantity_sold_before_promo AS bigint) * base_price ) AS total_product_revenue_before_campaign,
       
        SUM(
            CASE 
                WHEN promo_type = 'BOGOF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price / 2)
                WHEN promo_type = '50% OFF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price * 0.5)
                WHEN promo_type = '33% OFF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price * 0.67)
                WHEN promo_type = '500 Cashback' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price - 500)
                WHEN promo_type = '25% OFF' THEN 
                    CAST(quantity_sold_after_promo AS bigint) * (base_price * 0.75)
                ELSE
                    CAST(quantity_sold_after_promo AS bigint) * base_price
            END
        ) AS total_product_revenue_after_campaign
    FROM
    dim_products dp
  JOIN
    fact_events fe ON dp.product_code = fe.product_code
	group by dp.product_code,dp.product_name,dp.category)

	SELECT TOP 5
    product_name,
    category,
    ((total_product_revenue_after_campaign - total_product_revenue_before_campaign) * 100) / total_product_revenue_before_campaign AS ir_percentage
FROM
    cte
ORDER BY
    ir_percentage DESC; 
   



SELECT ds.city, COUNT(fe.event_id) AS num_events
FROM dim_stores ds
JOIN fact_events fe ON ds.store_id = fe.store_id
GROUP BY ds.city;
