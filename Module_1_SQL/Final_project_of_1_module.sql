/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: ***
 * Дата: 02.10.2025
*/



/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/
--CREATE VIEW ds_ecom.product_user_features AS
WITH filtered AS (
	SELECT
		*
	FROM ds_ecom.orders AS o
	JOIN ds_ecom.users AS us USING(buyer_id)
	WHERE o.order_status IN ('Доставлено', 'Отменено')
		AND us.region IN (
			SELECT
				u.region
			FROM ds_ecom.users AS u
			JOIN ds_ecom.orders AS o USING(buyer_id)
			GROUP BY u.region
			ORDER BY COUNT(o.buyer_id) DESC
			LIMIT 3
		)
), -- отфильтрованная таблица (только статусы "доставлено" или "отменено" и записи из топ 3 регионов)
first_payment AS (
	SELECT
		DISTINCT(order_id),
		FIRST_VALUE(payment_sequential) OVER (PARTITION BY order_id ORDER BY payment_sequential) AS payment_sequential
	FROM ds_ecom.order_payments
	JOIN filtered AS f USING(order_id)
	WHERE payment_type = 'денежный перевод'
), -- Первый тип оплаты (тут исправил, теперь payment_sequential может начинаться не с 1, а с любого другого числа)
promo_payment AS (
	SELECT
		pr.order_id,
		CASE
			WHEN SUM(pr.used_promo) > 0 THEN 1
			ELSE 0
		END AS binary_promo
	FROM (
		SELECT
			order_id,
			payment_type,
			CASE
				WHEN payment_type = 'промокод' THEN 1
				ELSE 0
			END AS used_promo
		FROM ds_ecom.order_payments
		) AS pr
	JOIN filtered AS f USING(order_id)
	GROUP BY pr.order_id
), -- уникальные заказы с бинарной классификацией оплаты промокодом (0 - не опалчивал, 1 - опличивал)
installments AS (
	SELECT
		order_id
	FROM ds_ecom.order_payments
	GROUP BY order_id
	HAVING SUM(CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END) > 0
), -- заказы, в которых использовалась рассрочка
orders_cost AS (
	SELECT
		oi.order_id,
		SUM(oi.price) + SUM(oi.delivery_cost) AS total_order_costs
	FROM filtered AS f
	JOIN ds_ecom.order_items AS oi USING(order_id)
	WHERE f.order_status = 'Доставлено'
	GROUP BY oi.order_id 
), -- Полная стоимость доставленных заказов
reviews AS (
	SELECT
		order_id,
		AVG(CASE
			WHEN review_score > 5 THEN review_score::FLOAT / 10.0
			ELSE review_score::FLOAT
		END) AS corrected_review_score
	FROM ds_ecom.order_reviews
	JOIN filtered AS f USING(order_id)
	GROUP BY order_id
), -- отзывы для каждого заказа (исправил, теперь сначала подсчитывается средний рейтинг для каждого заказа)
cancelled_orders AS (
	SELECT
		order_id
	FROM filtered
	WHERE order_status = 'Отменено'
), -- количество отмененных заказов для каждого пользователя
clients AS (
	SELECT
		u.user_id,
		u.region,
		MIN(f.order_purchase_ts) AS first_order_ts,
		MAX(f.order_purchase_ts) AS last_order_ts,
		MAX(f.order_purchase_ts) - MIN(f.order_purchase_ts) AS lifetime,
		COUNT(f.order_id) AS total_orders,
		AVG(r.corrected_review_score) AS avg_order_rating,
		SUM(CASE WHEN r.corrected_review_score IS NOT NULL THEN 1 ELSE 0 END) AS num_orders_with_rating,
		COUNT(co.order_id) AS num_canceled_orders,
		COUNT(co.order_id)::FLOAT / COUNT(f.order_id) AS canceled_orders_ratio,
		SUM(oc.total_order_costs) AS total_order_costs,
		SUM(oc.total_order_costs) / (COUNT(f.order_id) - COUNT(co.order_id)) AS avg_order_cost,
		COUNT(i.order_id) AS num_installment_orders,
		COALESCE(SUM(pp.binary_promo), 0) AS num_orders_with_promo,
		CASE WHEN SUM(fp.payment_sequential) > 0 THEN 1 ELSE 0 END AS used_money_transfer,
		CASE WHEN COUNT(i.order_id) > 0 THEN 1 ELSE 0 END AS used_installments,
		CASE WHEN COUNT(co.order_id) > 0 THEN 1 ELSE 0 END AS used_cancel
	FROM ds_ecom.users AS u
	JOIN filtered AS f USING(buyer_id)
	LEFT JOIN reviews AS r USING(order_id)
	LEFT JOIN cancelled_orders AS co USING(order_id)
	LEFT JOIN orders_cost AS oc USING(order_id)
	LEFT JOIN installments AS i USING(order_id)
	LEFT JOIN promo_payment AS pp USING(order_id)
	LEFT JOIN first_payment AS fp USING(order_id)
	GROUP BY u.user_id, u.region
)
SELECT
	*
FROM clients;



/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/


/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

-- Напишите ваш запрос тут
-- исправил этот запрос, теперь тут только уникальные пользователи, которые не зависят от региона заказа
WITH unique_users AS(
SELECT
	user_id,
	SUM(total_orders) AS total_orders,
	SUM(total_order_costs) AS total_order_costs
FROM ds_ecom.product_user_features
GROUP BY user_id
)
SELECT
	CASE
		WHEN total_orders = 1 THEN '1 order'
		WHEN total_orders < 6 THEN '2-5 orders'
		WHEN total_orders < 11 THEN '6-10 orders'
		WHEN total_orders >= 11 THEN '11 and more orders'
		ELSE 'Неизвестно'
	END AS segmentation,
	COUNT(user_id) AS total_users,
	ROUND(AVG(total_orders), 1) AS avg_total_orders,
	ROUND(SUM(total_order_costs) / SUM(total_orders), 1) AS avg_total_order_costs
FROM unique_users
GROUP BY segmentation
ORDER BY avg_total_orders;

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * Почти все клиенты маркетплейса (~96.9%) имеют только 1 заказ, т.е. компания имеет очень низкий возврат клиентов.
 * Также можно отметить что с увеличением среднего количества заказов уменьшается средняя стоимость заказа при сравнении сегмента с 1 заказом и 2-5 заказов,
 * сравнивать другие сегменты будет неккоректно из-за малого количества пользователей в этих сегментах
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

-- Напишите ваш запрос тут
SELECT
	ROW_NUMBER() OVER(ORDER BY avg_order_cost DESC) AS №, -- добавил ранжирование
	*
FROM ds_ecom.product_user_features
WHERE total_orders > 2
ORDER BY avg_order_cost DESC
LIMIT 15;
/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * В топ 15 пользователей с самым большим средним чеком заказа наблюдается большой размах в значении среднего чека, макс 14 716, мин 5 526
 * У 13 из 15 представленных пользователей количество заказов равно 3. Ни один из этих пользователей никогда не отменял заказ.
 * Только 1 из пользователей использовал промокод и 13 из 15 пользователей использовали рассрочку.
 * Все представленные пользователи входят в сегмент '2-5 orders'. Значения среднего стоимости заказа сильно отличается от среднего по сегменту
 * в котором находятся эти пользователи (3055), разница максимального полученгого значения со средним по сегменту составляет 11661, разница
 * минимального полученного значения со средним по сегменту составляет 2472.
*/


/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

-- Напишите ваш запрос тут
-- Добавил расчет средней цены заказа и представление долей в процентном виде
SELECT
	region,
	COUNT(user_id) AS total_users,
	SUM(total_order_costs) / SUM(total_orders) AS avg_order_cost,
	SUM(total_orders) AS total_orders,
	ROUND(SUM(num_installment_orders)::NUMERIC / SUM(total_orders) * 100, 2) || '%' AS installment_share,
	ROUND(SUM(num_orders_with_promo)::NUMERIC / SUM(total_orders) * 100, 2) || '%' AS promo_use_share,
	ROUND(SUM(used_cancel)::NUMERIC / COUNT(user_id) * 100, 2) || '%' AS canceled_share
FROM ds_ecom.product_user_features
GROUP BY region;

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * ~63.1% пользователей делают заказы из Москвы, остальные 36.9% распределены примерно равномерно между СпБ и Новосибирской областью.
 * Количество заказов по регионам распределено примерно также как и пользователи. Наибольший средний чек наблюдается у пользователей из
 * Санкт-Петербурга (3593), наименьший - у пользователей из Москвы (3140). Сравнивая среднюю стоимость заказа по сегментам (1 запрос) с полученными,
 * можно сделать вывод о том, что пользователи из Новосибирской области и Санкт-Петербуга чаще московских пользователей делают единичные заказы
 * с относительно высокой стоимостью. Пользователи из Москвы реже пользуются рассрочкой (47.7%) по сравнению с
 * СпБ и Новосибирской областью (~54.3%). В целом пользователи, которые хотя бы раз отменяли заказ имеют небольшую доли, однако стоит отметить
 * небольшую неравномерность: в Москве 0.06%, СпБ 0.04%, Новосибирская обл. 0.05%. Использование промокодов в каждом регионе распределно примерно одинаково.
*/


/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

-- Напишите ваш запрос тут
 -- Расчет new_avg_order_rating. здесь возможно не так понял, но если умножить средний рейтинг на кол-во заказов с оценками, и потом разделить
 -- на кол-во заказов с оценками получится тоже самое что и просто средний рейтинг
WITH unique_users AS(
SELECT
	user_id,
	MIN(first_order_ts) AS first_order_ts,
	MAX(last_order_ts) AS last_order_ts,
	SUM(total_orders) AS total_orders,
	SUM(total_order_costs) AS total_order_costs,
	AVG(avg_order_rating) AS avg_order_rating,
	CASE WHEN SUM(used_money_transfer) > 0 THEN 1 ELSE 0 END AS used_money_transfer,
	MAX(last_order_ts) - MIN(first_order_ts) AS lifetime,
	SUM(num_orders_with_rating) AS num_orders_with_rating
FROM ds_ecom.product_user_features
GROUP BY user_id
) -- теперь пользователи не дублируются
SELECT 
    TO_CHAR(DATE_TRUNC('month', first_order_ts), 'Month') AS month,
    COUNT(user_id) AS total_users,
    SUM(total_orders) AS total_orders,
    SUM(total_order_costs) / SUM(total_orders) AS avg_order_cost,
    SUM(avg_order_rating)::FLOAT / SUM(num_orders_with_rating) AS old_avg_order_rating, -- оставил для сравнения
    AVG(avg_order_rating)::FLOAT AS new_avg_order_rating, -- Добавил новый расчет рейтинга
    ROUND(SUM(used_money_transfer)::NUMERIC / COUNT(user_id) * 100, 2)  ||'%' AS used_money_transfer_share,
    DATE_TRUNC('minute', AVG(lifetime)) AS avg_lifetime,
    DATE_TRUNC('minute', AVG(NULLIF(lifetime, '0'::interval))) AS avg_lifetime_without_nulls
FROM unique_users
WHERE first_order_ts >= '2023-01-01' AND first_order_ts < '2024-01-01'
GROUP BY DATE_TRUNC('month', first_order_ts), TO_CHAR(DATE_TRUNC('month', first_order_ts), 'MM')
ORDER BY TO_CHAR(DATE_TRUNC('month', first_order_ts), 'MM');

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * Можно проследить тенденцию на увеличение количества пользователей и заказов в 2023 году по мере приближения 2024 г. Средняя стоимость заказа также увеличивалась в течение 2023.
 * Средняя оценка заказа и доля пользователей, использующих "денежный перевод" со временем почти что не изменяется.
 * Средний "жизненный цикл клиента" также сокращается с течением времени, что говорит о том, что время до повторного заказа, среди клиентов которые совершают повторные заказы - сокращается.
 * (В задании нет указаний что делать с нулями, мне показалось что логичнее убрать пользователей с 0, т.е. ориентировался на avg_lifetime_without_nulls)
 */
