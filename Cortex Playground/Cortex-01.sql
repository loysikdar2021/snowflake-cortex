SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'llama3-8b',      -- The Model
    'How old is the universe? And how do we know it?'  -- The Prompt
);

SELECT 
    O_ORDERKEY,
    O_COMMENT,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large', 
        'Classify the sentiment of this text as Positive, Negative, or Neutral and just return the values as Positive, Negative, or Neutral with one word: ' || O_COMMENT
    ) AS sentiment_analysis
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
LIMIT 50;

