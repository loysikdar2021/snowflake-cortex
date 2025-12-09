This flow diagram illustrates the end-to-end architecture you built, starting from the Environment Setup, moving through the ad-hoc AI/ML experiments, and finishing with the Automated RAG Pipeline and Streamlit Interface.

### **Detailed Architecture Flow**

```mermaid
graph TD
    %% Global Styles
    classDef db fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef func fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
    classDef auto fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    classDef ui fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px;

    subgraph "Phase 1: Environment Setup"
        SampleDB[("SNOWFLAKE_SAMPLE_DATA<br/>(Read Only)")]:::db
        PlayDB[("CORTEX_PLAYGROUND<br/>(Read/Write)")]:::db
        SampleDB -->|Copy/Reference| PlayDB
    end

    subgraph "Phase 2: Ad-Hoc GenAI"
        OrderText[Raw Order Comments]
        SentFunc{{Cortex.SENTIMENT}}:::func
        SumFunc{{Cortex.SUMMARIZE}}:::func
        TransFunc{{Cortex.TRANSLATE}}:::func

        SampleDB --> OrderText
        OrderText --> SentFunc --> SentResult[Sentiment Score]
        OrderText --> SumFunc --> SumResult[Summary]
        SumResult --> TransFunc --> TransResult[French Translation]
    end

    subgraph "Phase 3: Machine Learning"
        LineItem[LineItem Data]
        AggData[Aggregated Daily Sales]
        ForeMod{{Cortex.FORECAST}}:::func
        PredResult[Sales Predictions]

        SampleDB --> LineItem
        LineItem -->|Group By Date| AggData
        AggData --> ForeMod
        ForeMod -->|Predict Next 10 Days| PredResult
    end

    subgraph "Phase 5: Automated RAG Pipeline"
        AppSim[("INCOMING_ORDERS<br/>(Simulation Table)")]:::auto
        Stream[("ORDERS_STREAM<br/>(Change Data Capture)")]:::auto
        Task(("VECTORIZER_TASK<br/>(Serverless Schedule)")):::auto
        EmbedFunc{{Cortex.EMBED_TEXT}}:::func
        VectorDB[("ORDER_KNOWLEDGE_BASE<br/>(Vector Store)")]:::db

        AppSim -->|Insert New Row| Stream
        Stream -->|Trigger if Data Exists| Task
        Task -->|Call Embedding Model| EmbedFunc
        EmbedFunc -->|Insert Vector| VectorDB
    end

    subgraph "Phase 6: User Interface (Streamlit)"
        User((User))
        SiS[("Streamlit App<br/>(Python UI)")]:::ui
        VectorSearch{{VECTOR_L2_DISTANCE}}:::func
        LLM{{Cortex.COMPLETE}}:::func

        User -->|Asks Question| SiS
        SiS -->|1. Search Context| VectorSearch
        VectorDB -.->|Source Data| VectorSearch
        VectorSearch -->|2. Top Results| LLM
        SiS -->|3. Send Context + Prompt| LLM
        LLM -->|4. Generate Answer| SiS
        SiS -->|Display| User
    end

    %% Connect Phases where logical
    PlayDB -.-> VectorDB
```

### **Architecture Highlights**

1.  **Read-Only Source:** We successfully bridged the immutable `SNOWFLAKE_SAMPLE_DATA` by creating a "Playground" database to store our ML models and Vectors.
2.  **Serverless Compute:** Notice that **Cortex Functions** (Yellow nodes) handle the heavy lifting (Sentiment, Forecast, Embedding, Generation) without you managing GPU instances.
3.  **Event-Driven Architecture (Green Section):** The automation does not run on a rigid timer blindly; the **Task** checks the **Stream**. If no new orders arrive, the warehouse does not spin up, saving credits.
4.  **The "RAG Loop" (Purple Section):** The Streamlit app closes the loop by performing the "Retrieval" (Vector Distance) and "Generation" (Cortex Complete) in real-time based on the vectors created by your automated pipeline.