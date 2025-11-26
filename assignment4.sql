CREATE OR REPLACE TABLE steam_games_raw AS
SELECT * FROM read_json(
        '/Users/mac/assignment_4_DB/steam_2025_5k-dataset-games_20250831.json',
        format='auto',
        maximum_object_size=134217728
              );

DESCRIBE steam_games_raw;

CREATE OR REPLACE TABLE steam_reviews_raw AS
SELECT * FROM read_json(
        '/Users/mac/assignment_4_DB/Steam 5k Reviews Sept 2025.json',
        format='auto',
        maximum_object_size=134217728
              );

DESCRIBE steam_reviews_raw;

SELECT * FROM steam_games_raw LIMIT 1;

SELECT
    json_type(games) AS games_type,
    json_type(metadata) AS metadata_type,
FROM steam_games_raw
         LIMIT 10;

SELECT * FROM steam_reviews_raw LIMIT 1;

SELECT
    json_type(reviews) AS reviews_type,
    json_type(metadata) AS metadata_type,
FROM steam_reviews_raw
         LIMIT 10;


CREATE OR REPLACE TABLE games AS
SELECT
    unnest(games).appid::INTEGER AS appid,
    unnest(games).name_from_applist::VARCHAR AS name,
    unnest(games).app_details.data.type::VARCHAR AS type,
    unnest(games).app_details.data.is_free::BOOLEAN AS is_free,
    unnest(games).app_details.data.required_age::INTEGER AS required_age,
    unnest(games).app_details.data.short_description::VARCHAR AS description,
    unnest(games).app_details.data.platforms.windows::BOOLEAN AS windows,
    unnest(games).app_details.data.platforms.mac::BOOLEAN AS mac,
    unnest(games).app_details.data.platforms.linux::BOOLEAN AS linux,
    unnest(games).app_details.data.release_date.date::VARCHAR AS release_date,
    unnest(games).app_details.data.price_overview.final::INTEGER AS price_cents,
    unnest(games).app_details.data.metacritic.score::INTEGER AS metacritic_score,
    unnest(games).app_details.data.recommendations.total::INTEGER AS recommendations,
    unnest(games).app_details.data.developers AS developers,
    unnest(games).app_details.data.publishers AS publishers,
    unnest(games).app_details.data.genres AS genres,
    unnest(games).app_details.data.categories AS categories
FROM steam_games_raw;


CREATE OR REPLACE TABLE game_developers AS
SELECT appid, UNNEST(developers)::VARCHAR AS developer
FROM games WHERE developers IS NOT NULL;

CREATE OR REPLACE TABLE game_publishers AS
SELECT appid, UNNEST(publishers)::VARCHAR AS publisher
FROM games WHERE publishers IS NOT NULL;

CREATE OR REPLACE TABLE game_genres AS
SELECT appid, UNNEST(genres).id::INTEGER AS genre_id, UNNEST(genres).description::VARCHAR AS genre_name
FROM games WHERE genres IS NOT NULL;

CREATE OR REPLACE TABLE game_categories AS
SELECT appid, UNNEST(categories).id::INTEGER AS category_id, UNNEST(categories).description::VARCHAR AS category_name
FROM games WHERE categories IS NOT NULL;


CREATE OR REPLACE TABLE reviews AS
SELECT
    unnest(reviews).appid::INTEGER AS appid,
    unnest(reviews).review_data.query_summary.review_score::INTEGER AS review_score,
    unnest(reviews).review_data.query_summary.review_score_desc::VARCHAR AS review_score_desc,
    unnest(reviews).review_data.query_summary.total_positive::INTEGER AS total_positive,
    unnest(reviews).review_data.query_summary.total_negative::INTEGER AS total_negative,
    unnest(reviews).review_data.reviews AS reviews_array
FROM steam_reviews_raw;


CREATE OR REPLACE TABLE reviews_detail AS
SELECT
    appid,
    review_score,
    review_score_desc,
    UNNEST(reviews_array).recommendationid::VARCHAR AS review_id,
    UNNEST(reviews_array).author.steamid::VARCHAR AS author_id,
    UNNEST(reviews_array).author.playtime_forever::INTEGER AS playtime_minutes,
    UNNEST(reviews_array).language::VARCHAR AS language,
    UNNEST(reviews_array).review::VARCHAR AS review_text,
    UNNEST(reviews_array).voted_up::BOOLEAN AS recommended,
    UNNEST(reviews_array).votes_up::INTEGER AS helpful_votes,
    UNNEST(reviews_array).steam_purchase::BOOLEAN AS steam_purchase,
    to_timestamp(UNNEST(reviews_array).timestamp_created::BIGINT) AS created_at
FROM reviews;

ALTER TABLE games DROP COLUMN developers;
ALTER TABLE games DROP COLUMN publishers;
ALTER TABLE games DROP COLUMN genres;
ALTER TABLE games DROP COLUMN categories;

ALTER TABLE reviews DROP COLUMN reviews_array;

SELECT 'games' AS tbl, COUNT(*) AS cnt FROM games
UNION ALL SELECT 'game_developers', COUNT(*) FROM game_developers
UNION ALL SELECT 'game_publishers', COUNT(*) FROM game_publishers
UNION ALL SELECT 'game_genres', COUNT(*) FROM game_genres
UNION ALL SELECT 'game_categories', COUNT(*) FROM game_categories
UNION ALL SELECT 'reviews', COUNT(*) FROM reviews
UNION ALL SELECT 'reviews_detail', COUNT(*) FROM reviews_detail;



-- Part 2 — Analytical Insights

-- Top 20 games by number of reviews.
SELECT
    g.name,
    r.total_positive + r.total_negative AS total_reviews,
    r.total_positive,
    r.total_negative,
    r.review_score_desc
FROM reviews r
         JOIN games g ON r.appid = g.appid
ORDER BY total_reviews DESC
    LIMIT 20;
-- interpretation: this query shows us the top 20 games on Steam with the most number of reviews. We can clearly see that
-- Tom Clancy's Rainbow Six® Siege X, Rust and PAYDAY 2 are in top 3


-- Distribution of game release years.
SELECT
    CAST(SPLIT_PART(release_date, ', ', 2) AS INTEGER) AS release_year,
    COUNT(*) AS games_count
FROM games
WHERE release_date IS NOT NULL
  AND release_date != ''
  AND release_date LIKE '%, %'
GROUP BY release_year
ORDER BY games_count DESC;

-- Interpretation this query shows us the number of games that were released each year. From the result, we can see that
-- 2024 so far has been a year with the most games released, followed by 2025 and 2023
--

--Average price by genre (after JSON parsing and unnesting).
SELECT
    gg.genre_name,
    COUNT(DISTINCT g.appid) AS games_count,
    ROUND(AVG(g.price_cents) / 100.0, 2) AS avg_price_usd
FROM games g
         JOIN game_genres gg ON g.appid = gg.appid
WHERE g.price_cents IS NOT NULL AND g.price_cents > 0
GROUP BY gg.genre_name
ORDER BY avg_price_usd DESC;

-- Interpretation: This query shows us the average price of games by genre. From the result, we can see that the most
-- expensive genres are Animation & Modeling(other country genres does not count)

--Identify the most common tags across all games.
SELECT
    category_name,
    COUNT(*) AS games_count
FROM game_categories
GROUP BY category_name
ORDER BY games_count DESC
    LIMIT 20;

-- Interpretation this query shows us the most common tags across all games. From the result, we can see that
-- Single-player leads with 6692 game count, after that we have Family Sharing and Steam Achievements

-- Yearly review trends (number of reviews created each year)
SELECT
    EXTRACT(YEAR FROM created_at) AS review_year,
    COUNT(*) AS reviews_count
FROM reviews_detail
WHERE created_at IS NOT NULL
GROUP BY review_year
ORDER BY review_year DESC;

-- Interpretation this query shows us the number of reviews created each year. From the result, we can see that
-- 2025 has the highest number of reviews- 15480, followed by 2024 and 2023

-- The distribution of positive vs negative reviews
SELECT
    review_score_desc,
    COUNT(*) AS games_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM reviews
WHERE review_score_desc NOT LIKE '%user reviews%'
  AND review_score_desc != 'No user reviews'
GROUP BY review_score_desc
ORDER BY games_count DESC;
-- Interpretation this query shows us the distribution of positive vs negative reviews. From the result,
-- we can see that most games have positive reviews

-- Top 10 developers by the largest number of games
SELECT
    developer,
    COUNT(*) AS games_count
FROM game_developers
GROUP BY developer
ORDER BY games_count DESC
    LIMIT 10;
-- Interpretation this query shows us the top 10 developers by the largest number of games. From the result,
-- SmiteWorks USA, LLC leading with a huge distance from the competitors

-- The ratio of free-to-play vs paid games.
SELECT
    CASE
        WHEN is_free = true THEN 'Free'
        WHEN is_free = false THEN 'Paid'
        ELSE 'Unknown'
        END AS price_type,
    COUNT(*) AS games_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM games
GROUP BY price_type;
-- Interpretation this query shows us the ratio of free-to-play vs paid games. From the result, the 72.6% of
-- games on Steam are paid games, and only 18.9% are free


-- Platform that players use to play games (Windows, Mac, Linux).
SELECT
    CASE
        WHEN windows AND mac AND linux THEN 'Windows + Mac + Linux'
        WHEN windows AND mac THEN 'Windows + Mac'
        WHEN windows AND linux THEN 'Windows + Linux'
        WHEN mac AND linux THEN 'Mac + Linux'
        WHEN mac THEN 'Mac only'
        WHEN linux THEN 'Linux only'
        WHEN windows THEN 'Windows only'
        ELSE 'None'
        END AS platform_combo,
    COUNT(*) AS games_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM games
GROUP BY platform_combo
ORDER BY games_count DESC;
-- Interpretation this query shows us the platform that players use to play games. From the result, we can see that
-- Windows is the dominant platform with the most number of games available

