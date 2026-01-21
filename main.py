import sys
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from src.data_collection.database_setup import init_db
from src.data_collection.fetch_games import populate_teams, fetch_and_save_season
from src.features.rolling_stats import calculate_basic_rolling_features
from src.models.train_model import main_train
from src.models.predict_tonight import predict_tonight

def db_is_empty(engine):
    """Checks if the games table exists and has data."""
    inspector = inspect(engine)
    if 'games' not in inspector.get_table_names():
        return True
    
    # Check if there is at least one row in the games table
    with engine.connect() as conn:
        # Wrap the string in text()
        result = conn.execute(text("SELECT COUNT(*) FROM games")).scalar()
        return result == 0

def run_pipeline(seasons=[str(i) for i in range(2021, 2025)]):
    print("🚀 Starting NBA Betting Analytics Pipeline...")
    
    db_path = "data/nba_stats.db"
    db_exists = os.path.exists(db_path)
    engine = init_db()
    
    # Check if we should skip Phase 1 & 2
    if db_exists and not db_is_empty(engine):
        choice = input("📊 Database already has data. Rerun data collection? (y/n): ").lower()
        if choice != 'y':
            print("⏭️ Skipping Phase 1 & 2 (Data Collection & Transformation).")
            return

    # Phase 1 & 2 execution
    Session = sessionmaker(bind=engine)
    session = Session()

    print("\n--- Phase 1: Ingestion ---")
    populate_teams(session)
    for season in seasons:
        fetch_and_save_season(session, season)

    print("\n--- Phase 2: Transformation ---")
    calculate_basic_rolling_features(window_size=10)
    print("\n✅ Pipeline execution complete.")

def run_ai():
    print("\n--- Phase 3: AI Training ---")
    main_train()
    print("\n✅ AI training complete.\n--------------------------------\nPredictions:")
    predict_tonight()
    print("🏀 NBA Betting Analytics Pipeline finished successfully.")

if __name__ == "__main__":
    run_pipeline()
    run_ai()