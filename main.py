import sys
from sqlalchemy.orm import sessionmaker

# Importing the logic you've already written
from src.data_collection.database_setup import init_db
from src.data_collection.fetch_games import populate_teams, fetch_and_save_season
from src.features.rolling_stats import calculate_basic_rolling_features
from src.models.train_model import main_train

def run_pipeline(seasons=[str(i) for i in range(2021, 2025)]):
    """
    The main driver function. 
    Ensures the 'Pre-conditions' are met for each stage.
    """
    print("🚀 Starting NBA Betting Analytics Pipeline...")

    # Step 1: Initialize Database (The Memory Setup)
    engine = init_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Step 2: Ingest Raw Data
    print("\n--- Phase 1: Ingestion ---")
    populate_teams(session)
    for season in seasons:
        fetch_and_save_season(session, season)

    # Step 3: Feature Engineering (The Math)
    print("\n--- Phase 2: Transformation ---")
    calculate_basic_rolling_features(window_size=10)

    print("\n✅ Pipeline execution complete. Data is ready for the AI.")

if __name__ == "__main__":
    # You can pass command line arguments here if you want to get fancy
    # For now, we just run the last two seasons.
    run_pipeline()
    main_train()