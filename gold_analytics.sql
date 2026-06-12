-- ================================================
-- OTT Streaming Analytics Platform
-- Gold Layer Analytics Queries
-- Database: ott_gold_db
-- Query Engine: Amazon Athena
-- Author: Akash
-- ================================================


-- ────────────────────────────────────────────────
-- 1. Daily Active Users (Last 30 Days)
-- ────────────────────────────────────────────────
SELECT
    watch_date,
    daily_active_users,
    total_watch_events,
    avg_watch_mins,
    total_watch_mins / 60 AS total_watch_hours
FROM ott_gold_db.dau_metrics
ORDER BY watch_date DESC
LIMIT 30;


-- ────────────────────────────────────────────────
-- 2. Monthly Active Users (MAU)
-- ────────────────────────────────────────────────
SELECT
    DATE_TRUNC('month', watch_date)     AS month,
    SUM(daily_active_users)             AS monthly_active_users,
    SUM(total_watch_events)             AS monthly_watch_events,
    ROUND(AVG(avg_watch_mins), 1)       AS avg_daily_watch_mins,
    SUM(total_watch_mins) / 60          AS total_watch_hours
FROM ott_gold_db.dau_metrics
GROUP BY DATE_TRUNC('month', watch_date)
ORDER BY month DESC;


-- ────────────────────────────────────────────────
-- 3. Revenue by Subscription Plan
-- ────────────────────────────────────────────────
SELECT
    plan,
    SUM(monthly_revenue)                                    AS total_revenue,
    SUM(new_subscribers)                                    AS total_subscribers,
    ROUND(AVG(avg_revenue_per_user), 2)                    AS avg_revenue_per_user,
    ROUND(
        SUM(monthly_revenue) * 100.0 /
        SUM(SUM(monthly_revenue)) OVER(), 1
    )                                                       AS revenue_share_pct
FROM ott_gold_db.revenue_summary
GROUP BY plan
ORDER BY total_revenue DESC;


-- ────────────────────────────────────────────────
-- 4. Monthly Revenue Growth
-- ────────────────────────────────────────────────
SELECT
    month,
    SUM(monthly_revenue)    AS total_revenue,
    SUM(new_subscribers)    AS total_subscribers
FROM ott_gold_db.revenue_summary
GROUP BY month
ORDER BY month DESC
LIMIT 12;


-- ────────────────────────────────────────────────
-- 5. Top 10 Content by Watch Hours
-- ────────────────────────────────────────────────
SELECT
    title,
    type,
    genre,
    language,
    rating,
    total_views,
    watch_hours,
    unique_viewers,
    avg_completion_pct
FROM ott_gold_db.top_content
ORDER BY watch_hours DESC
LIMIT 10;


-- ────────────────────────────────────────────────
-- 6. Device Market Share
-- ────────────────────────────────────────────────
SELECT
    watch_device,
    unique_users,
    total_sessions,
    avg_watch_mins,
    avg_completion_pct,
    ROUND(
        total_sessions * 100.0 /
        SUM(total_sessions) OVER(), 1
    )                       AS session_share_pct
FROM ott_gold_db.device_analytics
ORDER BY total_sessions DESC;


-- ────────────────────────────────────────────────
-- 7. Genre Performance by Completion Rate
-- ────────────────────────────────────────────────
SELECT
    genre,
    COUNT(*)                        AS total_titles,
    ROUND(AVG(avg_completion_pct), 1) AS avg_completion_pct,
    SUM(total_views)                AS total_views,
    SUM(watch_hours)                AS total_watch_hours,
    SUM(unique_viewers)             AS total_unique_viewers
FROM ott_gold_db.top_content
GROUP BY genre
ORDER BY avg_completion_pct DESC;


-- ────────────────────────────────────────────────
-- 8. Movies vs Series Performance
-- ────────────────────────────────────────────────
SELECT
    type,
    COUNT(*)                            AS total_titles,
    SUM(total_views)                    AS total_views,
    SUM(watch_hours)                    AS total_watch_hours,
    ROUND(AVG(avg_completion_pct), 1)   AS avg_completion_pct,
    ROUND(AVG(rating), 1)               AS avg_rating
FROM ott_gold_db.top_content
GROUP BY type
ORDER BY total_views DESC;


-- ────────────────────────────────────────────────
-- 9. Peak Viewing Hours (from DAU metrics)
-- ────────────────────────────────────────────────
SELECT
    watch_date,
    daily_active_users,
    avg_watch_mins,
    CASE
        WHEN daily_active_users > (
            SELECT AVG(daily_active_users) FROM ott_gold_db.dau_metrics
        ) THEN 'Above Average'
        ELSE 'Below Average'
    END AS performance
FROM ott_gold_db.dau_metrics
ORDER BY daily_active_users DESC
LIMIT 20;


-- ────────────────────────────────────────────────
-- 10. Revenue Summary Executive View
-- ────────────────────────────────────────────────
SELECT
    SUM(monthly_revenue)        AS total_platform_revenue,
    SUM(new_subscribers)        AS total_subscribers,
    COUNT(DISTINCT plan)        AS active_plans,
    MAX(monthly_revenue)        AS peak_month_revenue,
    MIN(monthly_revenue)        AS lowest_month_revenue
FROM ott_gold_db.revenue_summary
WHERE plan != 'free';
