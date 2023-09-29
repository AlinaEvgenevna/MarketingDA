--ЧАСТЬ 1
--ОСНОВНЫЕ МЕТРИКИ

SELECT u.date, uniq_users, uniq_paying_users, uniq_couriers, uniq_active_couriers, orders, successful_orders
FROM
(SELECT time::date as date, 
count(DISTINCT user_id ) as uniq_users,
count(DISTINCT user_id) FILTER (WHERE order_id NOT IN (SELECT order_id from user_actions where action='cancel_order')) as uniq_paying_users
FROM user_actions
GROUP BY 1) u
LEFT JOIN
(SELECT time::date as date, 
count( DISTINCT(courier_id)) as uniq_couriers,
count( DISTINCT(courier_id)) FILTER (WHERE order_id NOT IN (SELECT order_id from user_actions where action='cancel_order')) as uniq_active_couriers
FROM courier_actions
GROUP BY 1) c USING(date)
LEFT JOIN
(SELECT creation_time::date as date,
count(distinct order_id) as orders,
count(distinct order_id) FILTER (WHERE order_id NOT IN (SELECT order_id from user_actions where action='cancel_order')) as successful_orders
FROM orders
GROUP BY 1) o USING(date)
ORDER BY 1




--ЧАСТЬ 2
--детали про заказы (основная ценность сервиса)


--1
--один или несколько заказов?

WITH q_orders as
(SELECT time::date as date, user_id,
    CASE 
    WHEN  count(order_id) = 1 THEN 'one_order'
    WHEN count(order_id) > 1 THEN 'several_orders'
    END AS q
    
FROM user_actions
WHERE order_id NOT IN (select order_id FROM user_actions where action ='cancel_order')
GROUP BY 1, 2),

status_count AS (SELECT date, 
count(q) FILTER(WHERE q ='one_order') as one_order,
count(q) FILTER(WHERE q ='several_orders') as several_orders
FROM q_orders
GROUP BY 1)

SELECT date,
ROUND(one_order::decimal*100/(one_order+several_orders),2) AS single_order_users_share, 
ROUND(several_orders::decimal*100/(one_order+several_orders),2) AS several_orders_users_share
FROM status_count
ORDER BY 1




--2
--коэффициент отмены заказов, распределние заказов по часам

SELECT DATE_PART('hour', creation_time)::int as hour, 
count(order_id) FILTER (WHERE order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')) as successful_orders,
count(order_id) FILTER(WHERE order_id IN (SELECT order_id FROM user_actions WHERE action='cancel_order')) as canceled_orders,
ROUND(count(order_id) FILTER(WHERE order_id IN (SELECT order_id FROM user_actions WHERE action='cancel_order'))/count(order_id)::decimal,3) as cancel_rate
FROM orders
GROUP BY 1
ORDER BY 1






--3
--сколько продуктов в заказе
select number_of_products,
       count(*) as frequency
from 
(SELECT order_id, array_length(product_ids,1) as number_of_products
FROM orders) t1
group by 1
order by 2 desc






--ЧАСТЬ 3
--МЕТРИКИ РОСТА (и его качества)


--1
--рост общего числа пользователей

SELECT date, new_users, new_couriers,
SUM(new_users) OVER(ORDER BY date)::int AS total_users,
SUM(new_couriers) OVER(ORDER BY date)::int AS total_couriers

FROM
(SELECT date, count(user_id) as new_users
FROM
(SELECT user_id, min(time::date) as date
FROM user_actions
GROUP BY user_id) AS min_dates_for_u
GROUP BY 1) u
JOIN
(SELECT date, count(courier_id) as new_couriers
FROM
(SELECT courier_id, min(time::date) as date
FROM courier_actions
GROUP BY courier_id) AS min_dates_for_c
GROUP BY 1) c
USING(date)






--2
--прирост (новых и общего числа пользователей)


SELECT date, new_users, new_couriers, total_users, total_couriers, 
ROUND((new_users - LAG(new_users) OVER(ORDER BY date))::decimal *100 /LAG(new_users) OVER(ORDER BY date),2) new_users_change, 
ROUND((new_couriers-LAG(new_couriers) OVER(ORDER BY date))::decimal *100 /LAG(new_couriers) OVER(ORDER BY date),2) new_couriers_change, 
ROUND((total_users-LAG(total_users) OVER(ORDER BY date))::decimal *100 /LAG(total_users) OVER(ORDER BY date),2) total_users_growth, 
ROUND((total_couriers-LAG(total_couriers) OVER(ORDER BY date))::decimal *100 /LAG(total_couriers) OVER(ORDER BY date),2) total_couriers_growth

from
(SELECT date, new_users, new_couriers,
SUM(new_users) OVER(ORDER BY date)::int AS total_users,
SUM(new_couriers) OVER(ORDER BY date)::int AS total_couriers

FROM
(SELECT date, count(user_id) as new_users
FROM
(SELECT user_id, min(time::date) as date
FROM user_actions
GROUP BY user_id) AS min_dates_for_u
GROUP BY 1) u
JOIN
(SELECT date, count(courier_id) as new_couriers
FROM
(SELECT courier_id, min(time::date) as date
FROM courier_actions
GROUP BY courier_id) AS min_dates_for_c
GROUP BY 1) c
USING(date)
) t1
ORDER BY 1;




--Часть 4
--Экономика

--ВЫРУЧКА
-- daily revenue, total_revenue, revenue_change

with unnested as
(SELECT creation_time, unnest(product_ids) as product_id
FROM orders
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')),

prices as
(select creation_time, u.product_id, price
FROM unnested u
JOIN products USING(product_id))


SELECT date, revenue,
SUM(revenue) OVER(order by date) as total_revenue,
ROUND((revenue - LAG(revenue) OVER(order by date))*100::decimal/ LAG(revenue) OVER(order by date),2) as revenue_change
FROM
(SELECT creation_time::date AS date, 
sum(price) as revenue
from prices
GROUP BY 1) revenues
ORDER BY 1;




--daily arpu, arppu, aov

with unnested as
(SELECT creation_time, order_id, unnest(product_ids) as product_id
FROM orders
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')),

add_prices as
(select creation_time, order_id, u.product_id, price
FROM unnested u
JOIN products USING(product_id)),

daily_revenues_n_orders as
(SELECT creation_time::date AS date, 
sum(price) as revenue,
count(distinct(order_id)) as n_orders
from add_prices
GROUP BY 1),

daily_users as
(SELECT time::date as date,
count(distinct user_id) as n_users,
count(distinct user_id) FILTER(WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')) as n_paying_users
FROM user_actions
GROUP BY 1),

abs_values as
(SELECT du.date, n_users, n_paying_users, n_orders, revenue
FROM daily_users du
JOIN daily_revenues_n_orders USING(date))

SELECT date, 
ROUND(revenue::decimal/n_users,2) as arpu, 
ROUND(revenue::decimal/n_paying_users, 2) as arppu, 
ROUND(revenue::decimal/n_orders,2) as aov
FROM abs_values
ORDER BY 1




--по товарам

with unnested as
(SELECT creation_time, order_id, unnest(product_ids) as product_id
FROM orders
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')),


add_prices as
(select creation_time, order_id, u.product_id, name, price
FROM unnested u
JOIN products USING(product_id)),

revenues as
(SELECT name, sum(price) as revenue,
ROUND(sum(price)*100::decimal/(SELECT sum(price) FROM add_prices),2) as share_in_revenue
FROM add_prices
GROUP BY 1)

SELECT 
CASE when share_in_revenue < 0.5 then 'ДРУГОЕ' 
     else name
     end AS product_name,
SUM(revenue) as revenue, SUM(share_in_revenue) as share_in_revenue
FROM revenues
GROUP BY 1
ORDER BY 2 desc




--доля выручки от новых и старых пользователей
--Добавлять на дашборд?

with unnested_products as (
    SELECT
      order_id,
      unnest(product_ids) as product_id
    FROM
      orders),
  
  add_prices as (
    SELECT
      order_id,
      u.product_id,
      price
    FROM
      unnested_products u
      join products using(product_id)
  ),
  
  
  order_prices as 
  (SELECT order_id, sum(price) as order_cost 
  FROM add_prices 
  GROUP BY 1),
 
 
min_dates as
(SELECT user_id, min(time::date) as min_date
FROM user_actions
      GROUP BY 1),


full_table as
(
 SELECT time::date as date, 
 user_id, 
 order_id, 
 order_cost,
 min_date
 FROM user_actions
 LEFT JOIN order_prices USING(order_id)
 LEFT JOIN min_dates USING(user_id)
 WHERE
      order_id not in (
        SELECT
          order_id
        FROM
          user_actions
        WHERE
          action = 'cancel_order'
      ) 
      )


    SELECT
      date,
      sum(order_cost) as revenue,
      sum(order_cost) FILTER (WHERE date = min_date) as new_users_revenue,
      ROUND(sum(order_cost) FILTER (WHERE date = min_date)*100::decimal/sum(order_cost),2) as new_users_revenue_share,
      100 - ROUND(sum(order_cost) FILTER (WHERE date = min_date)*100::decimal/sum(order_cost),2) as old_users_revenue_share
 
    FROM
     full_table
    GROUP BY 1
    ORDER BY 1






--Валовая прибыль


--для расчетов налогов и выручки
with product_economics as 
(SELECT product_id, price, 
CASE 
WHEN name IN ('сахар', 'сухарики', 'сушки', 'семечки', 
'масло льняное', 'виноград', 'масло оливковое', 
'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 
'овсянка', 'макароны', 'баранина', 'апельсины', 
'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 
'мука', 'шпроты', 'сосиски', 'свинина', 'рис', 
'масло кунжутное', 'сгущенка', 'ананас', 'говядина', 
'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 
'груши', 'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') THEN ROUND(price*0.1/(1+0.1), 2)
ELSE ROUND(price*0.2/(1+0.2),2) END as tax
FROM products),



--!выручка и налоги
revenues_taxes as
(SELECT date, sum(price) as revenue, sum(tax) as tax
FROM

(SELECT order_id, creation_time::date as date, unnest(product_ids) as product_id
FROM orders
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')) t1
JOIN product_economics USING(product_id)
GROUP BY 1),



--стоимость доставки
delivery_costs as
(SELECT time::date as date, 
CASE
WHEN time::date < '2022-09-01' THEN count(order_id) * 150  
ELSE count(distinct(order_id)) * 150
END as delivery_cost
FROM courier_actions
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')
AND action='deliver_order'
GROUP BY 1),


--стоимость сборки
package_costs as
(SELECT time::date as date, 
CASE
WHEN time::date < '2022-09-01' THEN count(order_id) * 140
ELSE count(order_id) * 115 
END as package_cost
FROM user_actions
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancel_order')
GROUP BY 1),

--премии
bonuses_costs as
(SELECT date, sum(bonus) as bonus_cost
FROM
(SELECT time::date as date, courier_id,
CASE
WHEN time::date < '2022-09-01' THEN 400
ELSE 500 
END as bonus
FROM courier_actions
WHERE action='deliver_order'
GROUP BY 1,2
HAVING COUNT(distinct order_id) >= 5) t1
GROUP BY 1),


all_cost as
(SELECT p.date, 
CASE 
WHEN date < '2022-09-01' THEN package_cost + delivery_cost + COALESCE(bonus_cost,0) + 120000
ELSE package_cost + delivery_cost + bonus_cost + 150000
END as costs
FROM package_costs p
LEFT JOIN bonuses_costs USING(date)
LEFT JOIN delivery_costs USING(date)
--ORDER BY 1
),

base_table as
(SELECT all_cost.date, revenue, costs::decimal, tax, revenue - costs - tax as gross_profit
FROM all_cost
JOIN revenues_taxes USING(date)
--ORDER BY 1
)

SELECT date, revenue, costs, tax, gross_profit,
SUM(revenue) OVER(ORDER BY date) as total_revenue, 
SUM(costs) OVER(ORDER BY date) as total_costs, 
SUM(tax) OVER(ORDER BY date) as total_tax, 
SUM(gross_profit) OVER(ORDER BY date) as total_gross_profit, 
ROUND(gross_profit::decimal*100/revenue,2) as gross_profit_ratio,
ROUND(SUM(gross_profit) OVER(ORDER BY date)::decimal*100/SUM(revenue) OVER(ORDER BY date),2) as total_gross_profit_ratio
FROM base_table
ORDER BY 1
