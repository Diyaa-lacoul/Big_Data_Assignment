"""
Reduce bus segments dataset to 30,000 rows using stratified sampling.
Preserves route diversity while reducing data size.
"""

import pandas as pd
import os
from sklearn.ensemble import RandomForestRegressor
import joblib

# Configuration
INPUT_FILE = '15913_107975_2026-01-25_16-02-26_current/bus_segments_extracted_20260207_080335.csv'
OUTPUT_FILE = '15913_107975_2026-01-25_16-02-26_current/bus_segments_30k.csv'
TARGET_ROWS = 30000

def reduce_dataset():
    # Read original data
    print("Loading dataset...")
    df = pd.read_csv(INPUT_FILE)
    
    print(f"Original dataset: {len(df):,} rows, {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    
    if len(df) <= TARGET_ROWS:
        print(f"Dataset already has {len(df)} rows (under {TARGET_ROWS}). No reduction needed.")
        return
    
    # Stratified sampling by line_name to preserve route diversity
    fraction = TARGET_ROWS / len(df)
    
    print(f"\nApplying stratified sampling (fraction: {fraction:.4f})...")
    
    sampled = df.groupby('line_name', group_keys=False).apply(
        lambda x: x.sample(frac=fraction, random_state=42)
    )
    
    # Adjust if slightly over target
    if len(sampled) > TARGET_ROWS:
        sampled = sampled.sample(n=TARGET_ROWS, random_state=42)
    
    # Sort for better organization
    sampled = sampled.sort_values(['line_name', 'from_sequence']).reset_index(drop=True)
    
    # Save reduced dataset
    sampled.to_csv(OUTPUT_FILE, index=False)
    
    # Statistics
    print(f"\n{'='*50}")
    print("REDUCTION SUMMARY")
    print(f"{'='*50}")
    print(f"Original rows:  {len(df):,}")
    print(f"Reduced rows:   {len(sampled):,}")
    print(f"Reduction:      {(1 - len(sampled)/len(df))*100:.1f}%")
    print(f"\nRoutes preserved: {sampled['line_name'].nunique()} unique lines")
    print(f"Original routes:  {df['line_name'].nunique()} unique lines")
    
    # Show sample distribution
    print(f"\nTop 10 routes by sample count:")
    route_counts = sampled['line_name'].value_counts().head(10)
    for route, count in route_counts.items():
        orig_count = len(df[df['line_name'] == route])
        print(f"  {route}: {count:,} samples (from {orig_count:,} original)")
    
    print(f"\nOutput saved to: {OUTPUT_FILE}")
    print(f"File size: {os.path.getsize(OUTPUT_FILE) / 1024 / 1024:.2f} MB")

    # Save model for GUI
    try:
        features = ['segment_distance_km', 'is_timing_point', 'is_pickup', 'lat_diff', 'lon_diff', 'heading_ns', 'heading_ew']
        X = sampled[features]
        y = sampled['runtime_seconds']
        model = RandomForestRegressor(random_state=42)
        model.fit(X, y)
        joblib.dump(model, 'travel_time_model.pkl')
        print("Model saved as travel_time_model.pkl for Streamlit GUI.")
    except Exception as e:
        print(f"Model training/saving failed: {e}")

if __name__ == "__main__":
    reduce_dataset()