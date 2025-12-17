# Architecture Diagram: Ford Predictive Warranty Analytics

```mermaid
graph TD
    subgraph "On-Premises / Simulation"
        A[Manufacturing Data<br/>(SQL/CSV)] -->|Batch| B(OneLake<br/>Lakehouse)
        C[Vehicle Telemetry<br/>(IoT Simulator)] -->|Stream| D(Eventstream)
    end

    subgraph "Microsoft Fabric Details"
        D -->|Ingest| E[KQL Database<br/>(Eventhouse)]
        E -->|OneLake Availability| B
        
        subgraph "Medallion Architecture"
            B -->|Bronze<br/>(Raw Parquet)| F(Silver<br/>Clean/Dedup)
            F -->|Transformation| G(Gold<br/>Star Schema)
        end

        subgraph "Analytics & AI"
            G -->|Direct Lake| H[Power BI<br/>Dashboard]
            G -->|Spark ML| I[ML Model<br/>(Random Forest)]
            I -->|PREDICT| J[Risk Scores<br/>(Table)]
            J -->|Trigger| K[Data Activator]
        end
    end

    K -->|Action| L[Salesforce Case]
```

## Storage Detail
- **Bronze**: Raw landing of JSON/CSV.
- **Silver**: Delta Tables with defined types (e.g., `Signal_Vibration_X` as double).
- **Gold**: `gold_vehicle_360` view joining `fact_telemetry` + `dim_vehicle` + `fact_claims`.
