import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from sqlalchemy import create_engine
from src.models.save_model import save_nba_model

# Loads feature data from SQL into a Pandas DataFrame
def load_data(db_path="sqlite:///data/nba_stats.db"):
    engine = create_engine(db_path)
    query = "SELECT * FROM model_ready_features"
    return pd.read_sql(query, engine)

# Prepares X (features) and y (target) and handles categorical mapping
def preprocess_data(df):
    # Convert W/L to binary: Home Win = 1, Home Loss = 0
    df['target'] = df['wl_home'].map({'W': 1, 'L': 0})
    
    # Select only the numerical features for training
    feature_cols = [
        'rolling_ppg_home', 'rolling_opp_ppg_home', 'rolling_win_pct_home', 'rest_days_home',
        'rolling_ppg_away', 'rolling_opp_ppg_away', 'rolling_win_pct_away', 'rest_days_away'
    ]
    
    X = df[feature_cols]
    y = df['target']
    return X, y

# Splitting data chronologically to respect the time-series nature of sports
def split_data_chronologically(X, y, test_size=0.2):
    split_idx = int(len(X) * (1 - test_size))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    return X_train, X_test, y_train, y_test

# Configures and trains the XGBoost Classifier
def train_xgboost(X_train, y_train):
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.05,
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss',
        gamma=0.3
    )
    model.fit(X_train, y_train)
    return model

# Evaluates performance and prints feature importance
def evaluate_model(model, X_test, y_test):
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    
    print(f"--- Model Performance ---")
    print(f"Accuracy: {accuracy:.2%}")
    print("\nClassification Report:")
    print(classification_report(y_test, predictions))
    
    print("\n--- Feature Importance (How the AI thinks) ---")
    importance = pd.DataFrame({
        'Feature': X_test.columns,
        'Importance': model.feature_importances_
    }).sort_values(by='Importance', ascending=False)
    print(importance)

def main_train():
    raw_df = load_data()
    X, y = preprocess_data(raw_df)
    
    X_train, X_test, y_train, y_test = split_data_chronologically(X, y)
    
    nba_model = train_xgboost(X_train, y_train)
    evaluate_model(nba_model, X_test, y_test)
    save_nba_model(nba_model)

# Main execution flow
if __name__ == "__main__":
    main_train()