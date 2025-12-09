-- Here is the continuation of the playbook: Phase 5: Automating the RAG Pipeline.

-- As a Data Architect, you know that RAG is only as good as the freshness of its data. We will now implement a Continuous Data Pipeline using Snowflake Streams (CDC) and Tasks (Serverless Scheduling) to automatically vectorise new data as it arrives.

-- Phase 5: Automating RAG (Streams & Tasks)
-- Architecture:

-- Source: INCOMING_ORDERS (Simulates your transactional app).

-- CDC: ORDERS_STREAM (Captures only new inserts).

-- Processing: VECTORIZER_TASK (Wakes up, generates embeddings via Cortex, loads Vector Store).

-- Destination: ORDER_KNOWLEDGE_BASE (Your Vector Store).

-- Step 1: Setup the Simulation Source
-- Since SNOWFLAKE_SAMPLE_DATA is immutable, we need a local table to simulate incoming application data.

USE SCHEMA CORTEX_PLAYGROUND.LABS;

-- Create a table to simulate fresh data arriving from an app
CREATE OR REPLACE TABLE INCOMING_ORDERS (
    O_ORDERKEY NUMBER,
    O_COMMENT VARCHAR
);

-- Step 2: Enable Change Data Capture (Stream)
-- Create a stream to track changes on the source table. This acts as a queue for our processing logic.
-- This stream will only show us rows that have been inserted but not yet consumed
CREATE OR REPLACE STREAM ORDERS_STREAM ON TABLE INCOMING_ORDERS;


-- Step 3: Define the Serverless Task
-- This task will check the stream every minute. If (and only if) there is data, it will trigger the Cortex Embedding function and load the Vector Store.

-- Note: We filter for METADATA$ACTION = 'INSERT' to only process new records.

CREATE OR REPLACE TASK VECTORIZER_TASK
    WAREHOUSE = CORTEX_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM') -- Only run if stream has data (Cost Optimization)
AS
    INSERT INTO ORDER_KNOWLEDGE_BASE (O_ORDERKEY, ORDER_TEXT, ORDER_VECTOR)
    SELECT
        O_ORDERKEY,
        O_COMMENT,
        -- Auto-generate embedding for the new record
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', O_COMMENT)
    FROM ORDERS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
    AND METADATA$ISUPDATE = FALSE;

-- Step 4: Activate the Pipeline
-- Tasks are created in a SUSPENDED state by default. We must enable it.

ALTER TASK VECTORIZER_TASK RESUME;

-- Step 5: Test the Automation (The "Live" Experiment)
-- Now, let's play the role of the application and insert a "new" order complaint.

-- 1. Simulate a new record
INSERT INTO INCOMING_ORDERS (O_ORDERKEY, O_COMMENT)
VALUES (
    999999,
    'The packages arrived water damaged.'
);

INSERT INTO INCOMING_ORDERS (O_ORDERKEY, O_COMMENT)
VALUES (
    999998,
    'The package arrived completely crushed.'
);
INSERT INTO INCOMING_ORDERS (O_ORDERKEY, O_COMMENT)
VALUES (
    999997,
    'The package  contents are water damaged.'
);


-- 2. Verify the Stream capture (Optional Debugging) If you run this immediately after inserting (before the minute is up), you will see the row waiting in the stream.

SELECT * FROM ORDERS_STREAM;

-- 3. Wait 60-90 seconds... then check the Vector Store The Task should have woken up, consumed the stream, generated the vector, and inserted it into your knowledge base.

SELECT *
FROM ORDER_KNOWLEDGE_BASE
WHERE O_ORDERKEY = 999999;

-------------------------------------------------Troubleshooting ---------------------------------------------------------
-- 1. Check the Task History (The "Logs")
-- The task might have failed, been skipped, or might still be scheduled. Run this to see the execution log:
SELECT
    QUERY_ID,
    NAME,
    STATE, -- Look for 'SUCCEEDED', 'FAILED', 'SKIPPED', or 'SCHEDULED'
    ERROR_MESSAGE,
    SCHEDULED_TIME,
    COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME=>'VECTORIZER_TASK',
    RESULT_LIMIT => 10
))
order by COMPLETED_TIME desc;

-- What to look for:

-- SKIPPED: The task ran but SYSTEM$STREAM_HAS_DATA returned false (the stream was empty).

-- FAILED: There is an error message (e.g., Warehouse issues, permission errors).

-- Empty Result: The task hasn't even tried to run yet.

-- 2. Check if the Data is Still in the Stream
-- If the Task didn't consume the data, the data should still be sitting in the stream waiting to be picked up.

SELECT * FROM ORDERS_STREAM;

-- If you see rows: The Task has not processed them yet. Proceed to Step 3.

-- If you see NO rows: The stream thinks the data was consumed. This implies the Task did run (successfully or failed during commit), or the data was consumed by another DML statement.

-- 3. Did you Resume the Task?
-- Tasks are created in a SUSPENDED state. If you forgot to run the resume command, it will never trigger.

-- Check status:

SHOW TASKS LIKE 'VECTORIZER_TASK';
-- Look at the 'state' column. It must say 'STARTED'.

-- If it says SUSPENDED, run this:

ALTER TASK VECTORIZER_TASK RESUME;

-- 4. Force Run the Task (The "Manual Override")
-- Instead of waiting for the 1-minute schedule, you can force the task to run immediately. This is excellent for debugging.

EXECUTE TASK VECTORIZER_TASK;

SELECT *
FROM ORDER_KNOWLEDGE_BASE
WHERE O_ORDERKEY < 1000000;

---------------------------------------------------------------------------------------------------------------------------

-- 4. Run a RAG query against the new data Now prove that the AI can "see" this new record immediately.
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'Context: ' || ORDER_TEXT || '\nQuestion: What is the issue with order 999999?'
    ) AS AI_RESPONSE
FROM ORDER_KNOWLEDGE_BASE
WHERE O_ORDERKEY = 999999;

-- Clean Up (Cost Control)
-- Since this is a playground, don't forget to suspend the task to avoid credit usage if you leave streams pending.

ALTER TASK VECTORIZER_TASK SUSPEND;

-- Architectural Note: In a production scenario, you would likely replace the manual INSERT with a Snowpipe loading data from S3/Azure Blob, or use Dynamic Tables if you prefer a declarative approach over the imperative Stream/Task approach.

-- Next Step: Would you like me to provide a Streamlit in Snowflake (SiS) script? This would give you a simple UI where you can type questions and see the RAG answers visually, rather than running SQL queries.