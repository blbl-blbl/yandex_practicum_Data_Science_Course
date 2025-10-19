/* Проект «Секреты Тёмнолесья»
 * Цель проекта: изучить влияние характеристик игроков и их игровых персонажей 
 * на покупку внутриигровой валюты «райские лепестки», а также оценить 
 * активность игроков при совершении внутриигровых покупок
 * 
 * Автор: ***
 * Дата: 24.09.2025
*/

-- Часть 1. Исследовательский анализ данных
-- Задача 1. Исследование доли платящих игроков

-- 1.1. Доля платящих пользователей по всем данным:
SELECT
	players_info.total_players,
	players_info.total_paying_players,
	players_info.total_paying_players::FLOAT / players_info.total_players::FLOAT AS share_paying_players
FROM (SELECT
	COUNT(*) AS total_players,
	(SELECT
		COUNT(*)
	FROM fantasy.users
	WHERE payer = 1) AS total_paying_players
FROM fantasy.users) AS players_info;

-- 1.2. Доля платящих пользователей в разрезе расы персонажа:
-- Количество игроков в каждой расе и количество платящих игроков в каждой расе
WITH race_players AS
(
    SELECT
        race_id,
        COUNT(*) AS total_race_players,
        SUM(payer) AS total_donate_players
    FROM fantasy.users
    GROUP BY race_id
)
SELECT 
	rp.race_id,
	r.race,
	rp.total_race_players,
	rp.total_donate_players,
	rp.total_donate_players::FLOAT / rp.total_race_players::FLOAT AS share_donate_players
FROM race_players AS rp
JOIN fantasy.race AS r ON r.race_id = rp.race_id;


-- Задача 2. Исследование внутриигровых покупок
-- 2.1. Статистические показатели по полю amount:
SELECT
	COUNT(*) AS trasaction_count,
	SUM(amount) AS sum_amount,
	MAX(amount) AS max_amount,
	MIN(amount) AS min_amount,
	AVG(amount) AS avg_amount,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median,
	STDDEV(amount)
FROM fantasy.events;

-- 2.2: Аномальные нулевые покупки:
SELECT
	COUNT(amount) AS total_events,
	(SELECT COUNT(*) AS total_events_with_nulls FROM fantasy.events WHERE amount = 0),
	(SELECT COUNT(*) FROM fantasy.events WHERE amount = 0) / COUNT(*)::FLOAT AS events_with_null_share
FROM fantasy.events;

-- 2.3: Популярные эпические предметы:
WITH events_without_nulls AS (
SELECT
	*
FROM fantasy.events
WHERE amount != 0
),
users_share AS (
	SELECT
	    item_code,
	    COUNT(DISTINCT id) AS unique_buyers,
	    COUNT(DISTINCT id)::FLOAT / (SELECT COUNT(DISTINCT id) FROM events_without_nulls) AS popularity_share
	FROM events_without_nulls
	GROUP BY item_code
)
SELECT
	i.game_items,
	COUNT(*) AS total_item_trancsactions,
	COUNT(*)::FLOAT / (SELECT COUNT(*) FROM events_without_nulls) AS item_share,
	us.popularity_share
FROM events_without_nulls AS ewn
JOIN users_share AS us ON us.item_code = ewn.item_code
JOIN fantasy.items AS i ON i.item_code = ewn.item_code 
GROUP BY ewn.item_code, us.popularity_share, i.game_items
ORDER BY us.popularity_share DESC;


-- Часть 2. Решение ad hoc-задачbи
-- Задача: Зависимость активности игроков от расы персонажа:
WITH events_without_nulls AS (
	SELECT
		id,
		COUNT(id) AS transation_count,
		SUM(amount) AS sum_amount,
		AVG(amount) AS avg_transact_sum
	FROM fantasy.events
	WHERE amount > 0
	GROUP BY id
), -- игроки совершившие транзакции за вычетом "аномалий"
total_players_for_race AS (
	SELECT
		r.race,
		COUNT(DISTINCT(u.id)) AS total_race_players
	FROM fantasy.users AS u
	JOIN fantasy.race AS r USING(race_id)
	GROUP BY r.race
), -- общее количество игроков по расам
players_for_race AS (
	SELECT
		r.race,
		COUNT(id) AS total_players_with_transaction
	FROM events_without_nulls 
	JOIN fantasy.users AS u USING(id)
	JOIN fantasy.race AS r ON r.race_id = u.race_id
	GROUP BY r.race_id
),-- Количество игроков с транзакциями по расам
paying_players_with_transaction AS (
	SELECT
		r.race,
		COUNT(DISTINCT(u.id)) AS total_paying_players
	FROM fantasy.users AS u
	JOIN fantasy.race AS r USING(race_id)
	JOIN events_without_nulls AS ev USING(id)
	WHERE u.payer = 1
	GROUP BY r.race
), -- Количество игроков которые являются платящими
avg_info AS (
	SELECT
		r.race,
		AVG(ev.transation_count) AS avg_transaction_count,
		AVG(ev.sum_amount) / AVG(ev.transation_count)  AS avg_transaction,
		AVG(ev.sum_amount) AS avg_sum_of_transaction
	FROM events_without_nulls AS ev
	JOIN fantasy.users AS u USING(id)
	JOIN fantasy.race AS r ON r.race_id = u.race_id
	GROUP BY r.race
) -- инфа по средним значениям
SELECT
	tpfr.race,
	tpfr.total_race_players,
	pfr.total_players_with_transaction,
	pfr.total_players_with_transaction::FLOAT / tpfr.total_race_players AS share_with_transaction_players,
	ppwt.total_paying_players::FLOAT / pfr.total_players_with_transaction AS share_paying_players_in_players_with_transact,
	ai.avg_transaction_count,
	ai.avg_transaction,
	ai.avg_sum_of_transaction
FROM total_players_for_race AS tpfr
JOIN players_for_race AS pfr ON pfr.race = tpfr.race
JOIN paying_players_with_transaction AS ppwt ON ppwt.race = tpfr.race
JOIN avg_info AS ai ON ai.race = tpfr.race
ORDER BY total_race_players 