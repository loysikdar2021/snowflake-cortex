-- Phase 1: Environment Setup
-- Since SNOWFLAKE_SAMPLE_DATA is read-only, we need a "Playground" database to store our ML models, vectors, and experiment results.

-- 1. Create the Playground Environment Run the following SQL to set up your workspace

USE ROLE SYSADMIN;

-- Create a database for experimentation
CREATE OR REPLACE DATABASE CORTEX_PLAYGROUND;
CREATE OR REPLACE SCHEMA CORTEX_PLAYGROUND.LABS;

-- Create a specific warehouse for ML/AI workloads (Standard or Medium recommended for ML)
CREATE OR REPLACE WAREHOUSE CORTEX_WH WITH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60;

USE WAREHOUSE CORTEX_WH;


-- Phase 2: Generative AI (LLM Functions)
-- We will use Snowflake Cortex's serverless LLM functions to analyze unstructured text found in the TPCH_SF1 schema (specifically the ORDERS table comments).

-- Goal: Perform Sentiment Analysis and Language Translation on order comments.

-- 1. Sentiment Analysis Let's determine if the comments left on orders are positive or negative.

SELECT
    O_ORDERKEY,
    O_COMMENT,
    -- Use Cortex Sentiment function (Returns score -1 to 1)
    SNOWFLAKE.CORTEX.SENTIMENT(O_COMMENT) AS SENTIMENT_SCORE,
    CASE
        WHEN SENTIMENT_SCORE > 0.2 THEN 'Positive'
        WHEN SENTIMENT_SCORE < -0.2 THEN 'Negative'
        ELSE 'Neutral'
    END AS SENTIMENT_LABEL
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
LIMIT 20;

-- 2. Summarization & Translation Now, let's pick a long comment, summarize it, and translate it to French.

SELECT
    O_ORDERKEY,
    O_COMMENT,
    -- Summarize the comment
    SNOWFLAKE.CORTEX.SUMMARIZE(O_COMMENT) AS SUMMARY,
    -- Translate the summary to French
    SNOWFLAKE.CORTEX.TRANSLATE(SUMMARY, 'en', 'fr') AS FRENCH_SUMMARY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
WHERE LENGTH(O_COMMENT) > 50 -- Filter for longer comments
LIMIT 5;

-- Phase 3: Machine Learning (Time-Series Forecasting)
-- We will use Cortex ML functions to forecast future sales based on historical data from the LINEITEM table.

-- Goal: Predict the daily total quantity of items shipped.

-- 1. Prepare the Historical Data We need to aggregate the sample data into a time-series format (Date + Value).

CREATE OR REPLACE TABLE CORTEX_PLAYGROUND.LABS.DAILY_SALES AS
SELECT
    L_SHIPDATE AS SHIP_DATE,
    SUM(L_QUANTITY) AS TOTAL_QUANTITY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
GROUP BY L_SHIPDATE
ORDER BY L_SHIPDATE;

-- Verify data
SELECT * FROM CORTEX_PLAYGROUND.LABS.DAILY_SALES LIMIT 10;

-- 2. Train the Forecast Model Snowflake manages the underlying ML infrastructure automatically.

-- Create a forecast model named 'SALES_FORECAST'
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST SALES_FORECAST (
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'CORTEX_PLAYGROUND.LABS.DAILY_SALES'),
    TIMESTAMP_COLNAME => 'SHIP_DATE',
    TARGET_COLNAME => 'TOTAL_QUANTITY'
);

-- 3. Generate Predictions Ask the model to predict the next 10 days.
CALL SALES_FORECAST!FORECAST(FORECASTING_PERIODS => 10);



-- Phase 4: RAG (Retrieval Augmented Generation)
-- We will build a simple RAG pipeline. We will treat the O_COMMENT column as our "Knowledge Base," turn it into vectors, and then ask questions about those comments.

-- Goal: Find orders regarding specific complaints (e.g., "delayed shipping") using semantic search, not keyword matching.

-- 1. Create a Vector Store We will select a subset of order comments and generate embeddings for them using EMBED_TEXT_768 (Snowflake's arctic-embed model).

CREATE OR REPLACE TABLE CORTEX_PLAYGROUND.LABS.ORDER_KNOWLEDGE_BASE AS
SELECT
    O_ORDERKEY,
    O_COMMENT AS ORDER_TEXT,
    -- Generate Vector Embedding (Dimensions: 768)
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', O_COMMENT) AS ORDER_VECTOR
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
LIMIT 10000; -- Limiting for speed in this demo

-- 2. Perform Vector Search (The "Retrieval") Let's find the most relevant order comment for a specific query: "Customer is angry about broken packaging."

SET USER_QUERY = 'Customer is angry about broken packaging';

-- Calculate distance between Query Vector and Document Vectors
SELECT
    O_ORDERKEY,
    ORDER_TEXT,
    VECTOR_L2_DISTANCE(
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', $USER_QUERY),
        ORDER_VECTOR
    ) AS DISTANCE_SCORE
FROM CORTEX_PLAYGROUND.LABS.ORDER_KNOWLEDGE_BASE
ORDER BY DISTANCE_SCORE ASC -- Lower distance means closer similarity
LIMIT 3;


-- 3. The "Generation" (Connecting Retrieval to LLM) This is the full RAG step. We take the top result from the search above and feed it to the LLM (mistral-large or llama3) to generate a human-readable response.

SELECT
    -- Pass the retrieved text as context to the LLM
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'You are a customer service agent. Based on the following order log, write a polite apology email to the customer. \n\n Context: ',
            ORDER_TEXT,
            '\n\n User Query: ',
            $USER_QUERY
        )
    ) AS DRAFT_EMAIL
FROM (
    -- Subquery: The Retrieval Logic from Step 2
    SELECT ORDER_TEXT
    FROM CORTEX_PLAYGROUND.LABS.ORDER_KNOWLEDGE_BASE
    ORDER BY VECTOR_L2_DISTANCE(
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', $USER_QUERY),
        ORDER_VECTOR
    ) ASC
    LIMIT 1
);

-- Summary of What You Built
-- Environment: A dedicated lab for AI experiments.

-- Cortex AI: Applied sentiment analysis and translation on raw text.

-- Cortex ML: Built a time-series forecasting model on shipping data.

-- RAG: Created a vector database from scratch and implemented a semantic search-to-generation pipeline.

