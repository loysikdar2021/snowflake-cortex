# Power BI Dashboard: Warranty Command Center

## Overview
A "Direct Lake" report connected to the `gold_vehicle_360` Semantic Model.

## Visuals Specification

### 1. Geospatial Risk Map
- **Type**: Azure Map
- **Data**: `dim_vehicle_manufacturing.Supplier_Location` (or derived lat-long)
- **Legend**: `Risk_Level` (High/Normal)
- **Purpose**: Identify if failures are clustering in specific regions (logistics) or just specific suppliers.

### 2. Supplier Performance Matrix
- **Type**: Scatter Plot
- **X-Axis**: `Odometer_KM` (Mileage)
- **Y-Axis**: `Vibration_X_G` (Sensor reading)
- **Legend**: `Fuel_Pump_Supplier`
- **Insight**: Users should see "Gamma_Tech" vehicles drifting up in vibration as mileage increases, while other suppliers stay flat.

### 3. Financial Impact Card
- **Type**: KPI Card
- **Measure**: `Potential_Savings`
- **DAX Formula**:
```dax
Potential_Savings = 
VAR Predicted_Failures = CALCULATE(COUNTROWS(prediction_results), prediction_results[Failure_Probability] > 0.7)
VAR Cost_Diff = 1500 - 400 -- Breakdown vs Scheduled
RETURN Predicted_Failures * Cost_Diff
```

## Data Connectivity
- **Mode**: Direct Lake
- **Refresh**: None (Real-time via OneLake)
