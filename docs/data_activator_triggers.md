# Data Activator Configuration

## Goal
Trigger operational workflows when a vehicle exhibits high failure probability or dangerous telemetry in real-time.

## Objects & Triggers

### Trigger 1: Real-Time Telemetry Alert
- **Source**: `RealTime_Telemetry` (Eventstream/KQL)
- **Condition**: 
  - `Signal_Vibration_X > 4.5` 
  - AND `DTC_List` contains "P0325"
- **Action**: 
  - **Type**: Fabric Item (Reflex) -> Power Automate
  - **Payload**: `{ VIN, Timestamp, Codes }`
  - **Output**: Send Email to Fleet Manager & Create Salesforce Case (Priority: High)

### Trigger 2: Predictive Risk Alert (Batch)
- **Source**: `prediction_results` (Lakehouse Table)
- **Condition**: 
  - `Failure_Probability` changes to > 0.8
- **Action**:
  - **Type**: Teams Message
  - **Recipient**: Field Service Team Channel
  - **Message**: "Vehicle {VIN} has crossed the 80% risk threshold. Schedule proactive pump replacement."

## Setup Instructions
1. Open "Data Activator" capability in Fabric.
2. Select "Get Data" -> "Eventstream".
3. Define the logic above in the No-Code interface.
