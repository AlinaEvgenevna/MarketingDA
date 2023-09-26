
--проверка дат

SELECT min(time)::date, max(time)::date
FROM user_actions;

--24/08/22	08/09/22



--dates, previous and next


SELECT user_id, time::date as day,
lag(time::date) OVER(PARTITION BY user_id ORDER BY time::date) as previous_active_day,
lead(time::date) OVER(PARTITION BY user_id ORDER BY time::date) as next_active_day
FROM user_actions
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancell_order')
GROUP BY user_id, time::date
ORDER BY 1,2





--Retained VS Churned by day
--по отношению к предедущему дню
with lag_lead as
(SELECT user_id, time::date as day,
lag(time::date) OVER(PARTITION BY user_id ORDER BY time::date) as previous_active_day,
lead(time::date) OVER(PARTITION BY user_id ORDER BY time::date) as next_active_day
FROM user_actions
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancell_order')
GROUP BY user_id, time::date
--ORDER BY 1,2
)

SELECT day, 
count(user_id) FILTER(WHERE day - previous_active_day =1) as retained_users,
count(user_id) FILTER(WHERE (next_active_day IS NULL OR next_active_day - previous_active_day > 1) and day <> (SELECT MAX(day) from lag_lead)) as churned_users
FROM lag_lead
GROUP BY 1
ORDER BY 1




--Retained, New, Returned, resurrected

with lag_lead as
(SELECT user_id, time::date as day,
lag(time::date) OVER(PARTITION BY user_id ORDER BY time::date) as previous_active_day,
lead(time::date) OVER(PARTITION BY user_id ORDER BY time::date) as next_active_day
FROM user_actions
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancell_order')
GROUP BY user_id, time::date
ORDER BY 1,2)

SELECT day, 
count(user_id) as total,
count(user_id) FILTER(WHERE previous_active_day IS NULL) as new_users,
count(user_id) FILTER(WHERE day - previous_active_day = 1) as retained_users,
count(user_id) FILTER(WHERE day - previous_active_day > 1 and day - previous_active_day < 14) as returned_users,
count(user_id) FILTER(WHERE day - previous_active_day >= 14) as  resurrected_users
FROM lag_lead
GROUP BY 1
ORDER BY 1


--проверка

SELECT user_id, day, previous_active_day, day - 1
FROM lag_lead
WHERE previous_active_day IS NOT NULL and previous_active_day > day - 1
ORDER BY 1,2
LIMIT 20





--когортный анализ

with start_days as
(SELECT user_id, time::date as day,
MIN(time::date) OVER(PARTITION BY user_id) as start_day
FROM user_actions
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action='cancell_order')
GROUP BY user_id, time::date
ORDER BY 1,2),

abs_values as
(SELECT start_day as cohort, 
day, 
count(user_id) as abs_value,
MAX(count(user_id)) OVER(PARTITION BY start_day) as start_value
FROM start_days
GROUP BY 1,2)

SELECT DATE_PART('month', cohort) as start_month, cohort, DATE_PART('month', day) as month, day, ROUND(abs_value::decimal*100/start_value)::int as retention
FROM abs_values
ORDER BY 1,2,3,4

