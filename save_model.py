import joblib

def save_nba_model(model, filename="nba_model.pkl"):
    # We use joblib because it is optimized for large NumPy arrays and XGBoost structures
    joblib.dump(model, f"models/{filename}")
    print(f"✅ Model saved to models/{filename}")

# Run this once at the end of your training script
# save_nba_model(nba_model)