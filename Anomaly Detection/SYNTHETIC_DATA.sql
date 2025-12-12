CREATE OR REPLACE TABLE LAB_CALL_CENTER_KM AS
WITH base_calls AS (
    SELECT
        SEQ8() AS CALL_ID,

        -- Random call start timestamp over 12 months from 2024-01-01
        DATEADD(
            'second',
            UNIFORM(0, 365*24*60*60, RANDOM()),
            TO_TIMESTAMP_NTZ('2024-01-01 00:00:00')
        ) AS CALL_START_TS,

        -- Agent & customer identifiers
        UNIFORM(1, 500, RANDOM())     AS AGENT_ID,
        UNIFORM(1, 100000, RANDOM())  AS CUSTOMER_ID,

        -- Randoms used for categorical choices and anomaly logic
        RANDOM() AS R1,
        RANDOM() AS R2,
        RANDOM() AS R3,
        RANDOM() AS R4
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
),

typed_calls AS (
    SELECT
        *,
        CASE
            WHEN R1 < 0.25 THEN 'GENERAL_INQUIRY'
            WHEN R1 < 0.45 THEN 'BILLING'
            WHEN R1 < 0.75 THEN 'TECH_SUPPORT'
            WHEN R1 < 0.90 THEN 'ACCOUNT_UPDATE'
            ELSE 'COMPLAINT'
        END AS CALL_TYPE,

        CASE
            WHEN R2 < 0.65 THEN 'STANDARD'
            WHEN R2 < 0.80 THEN 'TRANSFER'
            WHEN R2 < 0.92 THEN 'ESCALATED'
            WHEN R2 < 0.97 THEN 'CALLBACK'
            ELSE 'ABANDONED'
        END AS CALL_HANDLE_TYPE,

        CASE
            WHEN R3 < 0.70 THEN 'PHONE'
            WHEN R3 < 0.90 THEN 'CHAT'
            ELSE 'EMAIL'
        END AS CHANNEL,

        -- Agent tenure in months (impacts duration)
        UNIFORM(1, 60, RANDOM()) AS AGENT_TENURE_MONTHS,

        R4 AS R_ANOM
    FROM base_calls
),

base_metrics AS (
    SELECT
        *,
        -- Handle time (seconds) per call type
        CASE CALL_TYPE
            WHEN 'GENERAL_INQUIRY' THEN UNIFORM(240, 480, RANDOM())   -- 4-8 min
            WHEN 'BILLING'         THEN UNIFORM(300, 720, RANDOM())   -- 5-12 min
            WHEN 'TECH_SUPPORT'    THEN UNIFORM(480, 1200, RANDOM())  -- 8-20 min
            WHEN 'ACCOUNT_UPDATE'  THEN UNIFORM(180, 360, RANDOM())   -- 3-6 min
            WHEN 'COMPLAINT'       THEN UNIFORM(600, 1500, RANDOM())  -- 10-25 min
        END AS BASE_HANDLE_SEC,

        -- Knowledge system time (seconds) per call type
        CASE CALL_TYPE
            WHEN 'TECH_SUPPORT'     THEN UNIFORM(60, 420, RANDOM())   -- 1-7 min
            WHEN 'COMPLAINT'        THEN UNIFORM(30, 240, RANDOM())
            WHEN 'BILLING'          THEN UNIFORM(30, 240, RANDOM())
            WHEN 'GENERAL_INQUIRY'  THEN UNIFORM(0, 120, RANDOM())
            WHEN 'ACCOUNT_UPDATE'   THEN UNIFORM(0, 90, RANDOM())
        END AS BASE_KM_SEC,

        -- After-call work time (wrap-up)
        UNIFORM(30, 180, RANDOM()) AS BASE_ACW_SEC,   -- 0.5-3 min

        -- On-hold time
        UNIFORM(0, 300, RANDOM())  AS BASE_HOLD_SEC   -- 0-5 min
    FROM typed_calls
),

metrics_with_tenure AS (
    SELECT
        *,
        LEAST(1.5, GREATEST(0.9, 1.5 - (AGENT_TENURE_MONTHS / 100.0))) AS EFFICIENCY_FACTOR,

        BASE_HANDLE_SEC * LEAST(1.5, GREATEST(0.9, 1.5 - (AGENT_TENURE_MONTHS / 100.0)))
            AS TUNED_HANDLE_SEC,

        BASE_KM_SEC * LEAST(1.5, GREATEST(0.9, 1.5 - (AGENT_TENURE_MONTHS / 100.0)))
            AS TUNED_KM_SEC
    FROM base_metrics
),

with_anomalies AS (
    SELECT
        *,
        -- 10% of calls are ExtremeLongHandleTime anomalies, rest Normal
        CASE
            WHEN R_ANOM < 0.10 THEN 'ExtremeLongHandleTime'
            ELSE 'Normal'
        END AS ANOMALY_REASON,

        CASE
            WHEN R_ANOM < 0.10 THEN 1
            ELSE 0
        END AS IS_ANOMALY,

        CASE
            WHEN R_ANOM < 0.10 THEN                 -- Extremely long handle time
                 TUNED_HANDLE_SEC * 4 + UNIFORM(600, 1800, RANDOM())
            ELSE
                 TUNED_HANDLE_SEC
        END AS FINAL_HANDLE_SEC,

        -- KM time: just use tuned baseline (no KM anomalies here)
        TUNED_KM_SEC AS FINAL_KM_SEC
    FROM metrics_with_tenure
),

final_enriched AS (
    SELECT
        CALL_ID,
        CALL_START_TS,
        DATEADD('second', FINAL_HANDLE_SEC + BASE_ACW_SEC, CALL_START_TS) AS CALL_END_TS,
        TO_DATE(CALL_START_TS) AS CALL_DATE,
        TO_CHAR(CALL_START_TS, 'YYYY-MM') AS YEAR_MONTH,
        EXTRACT('DOW',  CALL_START_TS) AS DAY_OF_WEEK,
        EXTRACT('HOUR', CALL_START_TS) AS HOUR_OF_DAY,

        AGENT_ID,
        CUSTOMER_ID,
        CHANNEL,
        CALL_TYPE,
        CALL_HANDLE_TYPE,
        AGENT_TENURE_MONTHS,

        FINAL_HANDLE_SEC   AS HANDLE_TIME_SEC,
        BASE_ACW_SEC       AS AFTER_CALL_WORK_SEC,
        BASE_HOLD_SEC      AS HOLD_TIME_SEC,
        FINAL_KM_SEC       AS KM_TIME_SEC,

        CASE 
            WHEN FINAL_KM_SEC = 0 THEN 0
            ELSE GREATEST(1, CEIL(FINAL_KM_SEC / 60.0) + UNIFORM(0, 2, RANDOM()))
        END AS KM_SEARCH_COUNT,

        CASE 
            WHEN FINAL_KM_SEC = 0 THEN 0
            ELSE GREATEST(1, CEIL(FINAL_KM_SEC / 120.0) + UNIFORM(0, 1, RANDOM()))
        END AS KM_ARTICLES_VIEWED,

        CASE 
            WHEN CALL_HANDLE_TYPE = 'ABANDONED' THEN 0
            WHEN ANOMALY_REASON = 'ExtremeLongHandleTime' THEN 0
            ELSE 1
        END AS IS_RESOLVED,

        CASE 
            WHEN CALL_HANDLE_TYPE = 'ESCALATED'
                 OR ANOMALY_REASON = 'ExtremeLongHandleTime'
            THEN 1
            ELSE 0
        END AS IS_ESCALATED,

        IS_ANOMALY,
        ANOMALY_REASON
    FROM with_anomalies
)
SELECT * FROM final_enriched;


SELECT ANOMALY_REASON, COUNT(*) AS ROWS_COUNT, 
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM LAB_CALL_CENTER_KM), 2) AS PCT
FROM LAB_CALL_CENTER_KM
GROUP BY ANOMALY_REASON;


SELECT 
    ANOMALY_REASON,
    COUNT(*) AS ROWS_COUNT,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM LAB_CALL_CENTER_KM), 2) AS PCT
FROM LAB_CALL_CENTER_KM
GROUP BY ANOMALY_REASON;

-- 1) Create a temp table of ExtremeLongHandleTime rows we want to KEEP
CREATE OR REPLACE TEMP TABLE TMP_EXTREME_KEEP AS
WITH total AS (
    SELECT COUNT(*) AS total_rows
    FROM LAB_CALL_CENTER_KM
),
ranked AS (
    SELECT
        t.CALL_ID,
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn,
        (SELECT FLOOR(0.10 * total_rows) FROM total) AS target_extreme
    FROM LAB_CALL_CENTER_KM t
    WHERE t.ANOMALY_REASON = 'ExtremeLongHandleTime'
)
SELECT CALL_ID
FROM ranked
WHERE rn <= target_extreme;   -- keep only 10% of total rows as extreme


-- 2) Demote all other ExtremeLongHandleTime rows to Normal
UPDATE LAB_CALL_CENTER_KM t
SET 
    ANOMALY_REASON = 'Normal',
    IS_ANOMALY     = 0
    -- Optional: scale handle time down so they’re not crazy outliers anymore
    -- , HANDLE_TIME_SEC = GREATEST(60, HANDLE_TIME_SEC / 4)
    -- , CALL_END_TS     = DATEADD('second', HANDLE_TIME_SEC + AFTER_CALL_WORK_SEC, CALL_START_TS)
WHERE t.ANOMALY_REASON = 'ExtremeLongHandleTime'
  AND t.CALL_ID NOT IN (SELECT CALL_ID FROM TMP_EXTREME_KEEP);

SELECT 
    ANOMALY_REASON,
    COUNT(*) AS ROWS_COUNT,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM LAB_CALL_CENTER_KM), 2) AS PCT
FROM LAB_CALL_CENTER_KM
GROUP BY ANOMALY_REASON;

