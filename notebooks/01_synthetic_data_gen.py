# 01_synthetic_data_gen.py
# Purpose: Generate synthetic vehicle, telemetry, and warranty data for Ford Predictive Warranty Analytics prototype.
# This script is designed to be runnable locally for verification or copied into a Fabric Notebook.

import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
from faker import Faker
import os

# Ensure reproducibility
fake = Faker()
Faker.seed(42)
np.random.seed(42)

# Configuration
NUM_VEHICLES = 1000  # Reduced for local testing, scale to 100,000 for Fabric
OUTPUT_DIR = "data/raw"

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

# --- Block 2: Manufacturing Data (The Victims) ---
def generate_manufacturing_data(num_records):
    print(f"Generating {num_records} manufacturing records...")
    suppliers = ['Supplier_Alpha', 'Supplier_Beta', 'Gamma_Tech']
    supplier_weights = [0.40, 0.40, 0.20] # Gamma is 20% of fleet and problematic

    plants = ['KCAP', 'DTP', 'OHAP', 'MAP']
    
    data = {
        'vin': [fake.unique.bothify(text='1FAHP3?????######').upper() for _ in range(num_records)],
        'production_date': [fake.date_between(start_date='-3y', end_date='-1y') for _ in range(num_records)],
        'assembly_plant': np.random.choice(plants, num_records),
        'fuel_pump_supplier': np.random.choice(suppliers, num_records, p=supplier_weights),
        'engine_series': np.random.choice(['V6_EcoBoost', 'V8_Coyote', 'I4_Hybrid'], num_records),
        'model_year': np.random.choice([2023, 2024, 2025], num_records)
    }
    return pd.DataFrame(data)

# --- Block 3: Telemetry Generation with Anomaly Injection (The Signal) ---
def simulate_telemetry(df_mfg):
    print("Generating telemetry data with anomaly injection...")
    telemetry_records = []
    
    for _, row in df_mfg.iterrows():
        # Base physics
        mileage = np.random.uniform(500, 60000)
        base_vibration = np.random.normal(1.0, 0.2) # Normal operation
        rail_pressure = np.random.normal(2000, 50)  # PSI
        
        dtc_codes = []
        
        # INJECT DEFECT: If Gamma_Tech, degrade performance based on mileage
        if row['fuel_pump_supplier'] == 'Gamma_Tech':
            # Probability of defect manifesting
            if np.random.random() < 0.30: 
                # Vibration increases with mileage
                base_vibration += (mileage / 10000) * 1.5 
                # Pressure drops as vibration kills the pump
                rail_pressure -= (mileage / 10000) * 200
                
        # Trigger DTC if physics are violated
        if rail_pressure < 1200:
            dtc_codes.append('P0087') # Low Fuel Pressure
        if base_vibration > 4.5:
            dtc_codes.append('P0325') # Knock Sensor / Vibration

        telemetry_records.append({
            'vin': row['vin'],
            'odometer_km': mileage,
            'vibration_x_g': base_vibration,
            'fuel_rail_pressure_psi': rail_pressure,
            'active_dtcs': dtc_codes, # List stored as object/string for CSV
            'snapshot_timestamp': datetime.now() - timedelta(days=60)
        })

    return pd.DataFrame(telemetry_records)

# --- Block 4: Claims Generation (The Label) ---
def generate_claims(telemetry_df):
    print("Generating warranty claims based on telemetry failures...")
    claims = []
    # Identify vehicles with the specific failure mode
    # We look for P0087 (Low Pressure) as the definitive failure signal
    
    # Helper to check if P0087 is in the list
    def has_failure(dtc_list):
        return 'P0087' in dtc_list

    failed_vehicles = telemetry_df[telemetry_df['active_dtcs'].apply(has_failure)]
    
    for _, row in failed_vehicles.iterrows():
        # Not all failures result in a claim (80% capture rate)
        if np.random.random() < 0.80:
            claims.append({
                'claim_id': str(uuid.uuid4()),
                'vin': row['vin'],
                'claim_date': row['snapshot_timestamp'] + timedelta(days=np.random.randint(1, 14)),
                'primary_defect': 'Fuel Pump Failure',
                'repair_cost': np.random.uniform(400, 1200),
                'labor_hours': np.random.uniform(2, 5)
            })
    return pd.DataFrame(claims)

def main():
    ensure_output_dir()
    
    # 1. Generate Vehicles
    df_mfg = generate_manufacturing_data(NUM_VEHICLES)
    print(f"Manufacturing Data: {df_mfg.shape}")
    df_mfg.to_csv(f"{OUTPUT_DIR}/dim_vehicle_manufacturing.csv", index=False)
    
    # 2. Generate Telemetry
    df_telemetry = simulate_telemetry(df_mfg)
    print(f"Telemetry Data: {df_telemetry.shape}")
    df_telemetry.to_csv(f"{OUTPUT_DIR}/fact_telemetry_iot.csv", index=False)
    
    # 3. Generate Claims
    df_claims = generate_claims(df_telemetry)
    print(f"Claims Data: {df_claims.shape}")
    df_claims.to_csv(f"{OUTPUT_DIR}/fact_warranty_claims.csv", index=False)
    
    print("\nData generation complete. Files saved to 'data/raw/'.")

if __name__ == "__main__":
    main()
