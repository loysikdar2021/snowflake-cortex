# 02_transformation_silver.py
# Purpose: Cleanse and standardize raw data into the "Silver" layer.
# In Fabric, this would likely be a Delta Table write. Here we use CSV/Parquet for local simulation.

import pandas as pd
import os

INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/silver"

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def process_manufacturing():
    print("Processing Manufacturing Data...")
    df = pd.read_csv(f"{INPUT_DIR}/dim_vehicle_manufacturing.csv")
    
    # 1. Type Casting
    df['production_date'] = pd.to_datetime(df['production_date'])
    
    # 2. Deduplication (Critical for standard dimensions)
    df = df.drop_duplicates(subset=['vin'], keep='last')
    
    # 3. Standardization
    df['assembly_plant'] = df['assembly_plant'].str.upper()
    
    print(f"Manufacturing Silver Shape: {df.shape}")
    df.to_csv(f"{OUTPUT_DIR}/dim_vehicle_manufacturing.csv", index=False)
    return df

def process_telemetry():
    print("Processing Telemetry Data...")
    df = pd.read_csv(f"{INPUT_DIR}/fact_telemetry_iot.csv")
    
    # 1. Type Casting
    df['snapshot_timestamp'] = pd.to_datetime(df['snapshot_timestamp'])
    df['vibration_x_g'] = df['vibration_x_g'].astype(float)
    df['fuel_rail_pressure_psi'] = df['fuel_rail_pressure_psi'].astype(float)
    
    # 2. Handling Arrays (DTC codes comes as string from CSV)
    # In a real Spark DF this would be native ArrayType.
    # We leave it as string for CSV simplicity or eval if needed.
    
    print(f"Telemetry Silver Shape: {df.shape}")
    df.to_csv(f"{OUTPUT_DIR}/fact_telemetry_iot.csv", index=False)
    return df

def process_claims():
    print("Processing Claims Data...")
    try:
        df = pd.read_csv(f"{INPUT_DIR}/fact_warranty_claims.csv")
    except FileNotFoundError:
        print("No claims data found (could be empty batch). Creating empty DF.")
        return
        
    if df.empty:
        return

    # 1. Type Casting
    df['claim_date'] = pd.to_datetime(df['claim_date'])
    df['repair_cost'] = df['repair_cost'].astype(float)
    
    # 2. Logic Check: Claims cannot be in future (relative to run)
    df = df[df['claim_date'] <= pd.Timestamp.now()]
    
    print(f"Claims Silver Shape: {df.shape}")
    df.to_csv(f"{OUTPUT_DIR}/fact_warranty_claims.csv", index=False)
    return df

def main():
    ensure_output_dir()
    process_manufacturing()
    process_telemetry()
    process_claims()
    print("\nSilver layer transformation complete. Files saved to 'data/silver/'.")

if __name__ == "__main__":
    main()
