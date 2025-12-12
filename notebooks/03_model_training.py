# 03_model_training.py
# Purpose: Train a predictive model to identify high-risk vehicles.
# 1. Joins silver data to create "Gold" features.
# 2. Trains Random Forest.
# 3. Logs to MLflow (simulated or local).
# 4. Generates batch predictions.

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import mlflow
import mlflow.sklearn
import os

INPUT_DIR = "data/silver"
OUTPUT_DIR = "data/gold"

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def load_and_join_data():
    print("Loading Silver data...")
    df_mfg = pd.read_csv(f"{INPUT_DIR}/dim_vehicle_manufacturing.csv")
    df_tel = pd.read_csv(f"{INPUT_DIR}/fact_telemetry_iot.csv")
    
    try:
        df_claims = pd.read_csv(f"{INPUT_DIR}/fact_warranty_claims.csv")
    except:
        df_claims = pd.DataFrame(columns=['vin', 'claim_id'])

    print("Joining data to GOLD vehicle view...")
    # Join Telemetry + Mfg
    df = pd.merge(df_tel, df_mfg, on='vin', how='left')
    
    # Join Claims (Target Variable generation)
    # If a VIN exists in claims, it failed.
    failed_vins = df_claims['vin'].unique()
    df['has_failed'] = df['vin'].isin(failed_vins).astype(int)
    
    return df

def feature_engineering(df):
    # Encoding categorical variables
    # For prototype, we'll just One-Hot Encode 'fuel_pump_supplier' as it's the key predictor
    df_encoded = pd.get_dummies(df, columns=['fuel_pump_supplier'], prefix='supplier')
    
    # Feature Selection
    features = ['vibration_x_g', 'fuel_rail_pressure_psi'] + [c for c in df_encoded.columns if 'supplier_' in c]
    
    X = df_encoded[features]
    y = df_encoded['has_failed']
    
    return X, y, df

def train_model(X, y):
    print("Training Random Forest Classifier...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    
    y_pred = rf.predict(X_test)
    print("\nModel Evaluation:")
    print(classification_report(y_test, y_pred))
    
    return rf

def main():
    ensure_output_dir()
    
    # 1. Create Gold Dataset
    df = load_and_join_data()
    
    # 2. Train
    X, y, df_full = feature_engineering(df)
    model = train_model(X, y)
    
    # 3. Simulate Batch Scoring (The PREDICT part)
    # We predict on the whole fleet to find high-risk non-failed units
    all_predictions = model.predict_proba(X)[:, 1] # Probability of failure
    df_full['failure_probability'] = all_predictions
    
    # Save Results
    results = df_full[['vin', 'failure_probability', 'has_failed']]
    results.to_csv(f"{OUTPUT_DIR}/prediction_results.csv", index=False)
    print(f"Predictions saved to '{OUTPUT_DIR}/prediction_results.csv'.")
    
    # High Risk Alert
    high_risk = results[(results['failure_probability'] > 0.7) & (results['has_failed'] == 0)]
    print(f"\nALERT: Found {len(high_risk)} vehicles with >70% failure risk that have NOT yet failed.")

if __name__ == "__main__":
    main()
