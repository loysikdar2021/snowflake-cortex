-- Gold Layer View Definition
-- This SQL is intended to be run in the Fabric SQL Analytics Endpoint

CREATE VIEW gold_vehicle_360 AS
SELECT 
    t.vin,
    t.snapshot_timestamp AS event_timestamp,
    t.vibration_x_g,
    t.fuel_rail_pressure_psi,
    m.fuel_pump_supplier,
    m.assembly_plant,
    m.model_year,
    -- Label Generation for ML
    CASE WHEN c.claim_id IS NOT NULL THEN 1 ELSE 0 END AS has_failed,
    c.repair_cost,
    c.primary_defect
FROM 
    fact_telemetry_iot t
JOIN 
    dim_vehicle_manufacturing m ON t.vin = m.vin
LEFT JOIN 
    fact_warranty_claims c ON t.vin = c.vin 
    AND t.snapshot_timestamp < c.claim_date -- Only data BEFORE failure is valid for prediction? 
    -- Actually for training we want to know if it EVER failed associated with this telemetry?
    -- For "Time to Failure" models we need careful filtering. 
    -- For this binary classification "Will it fail?", we just tag the VIN.
