import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from src.data_collection.database_setup import init_db

Base = declarative_base()

# This class defines the "Contract" for the SQL table
class Features(Base):
    __tablename__ = 'model_ready_features'
    # Removed the 'pd.' prefixes. These are SQLAlchemy types.
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    rolling_ppg = Column(Float)
    rolling_opp_ppg = Column(Float)
    rolling_win_pct = Column(Float)
    rest_days = Column(Integer)

def make_sql_table():
    engine = init_db()
    # This acts like a 'malloc' for your SQL table structure
    Base.metadata.create_all(engine)


def calculate_basic_rolling_features(window_size=10):
    engine = create_engine('sqlite:///data/nba_stats.db')
    
    # 1. Load basic games (no advanced stats needed)
    query = "SELECT game_id, game_date, home_team_id, away_team_id, home_pts, away_pts, wl_home FROM games ORDER BY game_date ASC"
    df = pd.read_sql(query, engine)
    df['game_date'] = pd.to_datetime(df['game_date'])
    
    all_teams = pd.concat([df['home_team_id'], df['away_team_id']]).unique()
    all_team_features = []

    for team_id in all_teams:
        # Filter games where this team played
        team_games = df[(df['home_team_id'] == team_id) | (df['away_team_id'] == team_id)].copy()
        
        # Calculate Team-Specific Points and Opponent Points for each game
        # Using a ternary-style approach: If team is home, take home_pts, else away_pts
        team_games['team_pts'] = team_games.apply(
            lambda x: x['home_pts'] if x['home_team_id'] == team_id else x['away_pts'], axis=1
        )
        team_games['opp_pts'] = team_games.apply(
            lambda x: x['away_pts'] if x['home_team_id'] == team_id else x['home_pts'], axis=1
        )
        
        # Binary Win column: 1 if the team won, 0 if lost
        team_games['is_win'] = team_games.apply(
            lambda x: 1 if (x['home_team_id'] == team_id and x['wl_home'] == 'W') or 
                           (x['away_team_id'] == team_id and x['wl_home'] == 'L') else 0, axis=1
        )

        # 2. THE SLIDING WINDOW (Strictly looking at previous games)
        # We shift(1) to ensure the current game isn't included in its own average
        team_games['rolling_ppg'] = team_games['team_pts'].shift(1).rolling(window=window_size).mean()
        team_games['rolling_opp_ppg'] = team_games['opp_pts'].shift(1).rolling(window=window_size).mean()
        team_games['rolling_win_pct'] = team_games['is_win'].shift(1).rolling(window=window_size).mean()
        
        # 3. Rest Days (Time Delta)
        team_games['last_game_date'] = team_games['game_date'].shift(1)
        team_games['rest_days'] = (team_games['game_date'] - team_games['last_game_date']).dt.days
        
        # Clean up: Drop rows with NaN (the first 10 games of the season)
        features = team_games.dropna(subset=['rolling_ppg']).copy()
        features['team_id'] = team_id
        
        all_team_features.append(features[['game_id', 'team_id', 'rolling_ppg', 'rolling_opp_ppg', 'rolling_win_pct', 'rest_days']])

    # 4. Final Join: Create a single row per game with BOTH Home and Away features
    combined_features = pd.concat(all_team_features)
    
    # We need to pivot this so each Game_ID has Home_Rolling_PPG and Away_Rolling_PPG
    # This is essentially a Hash Join on Game_ID
    game_base = df[['game_id', 'game_date', 'home_team_id', 'away_team_id', 'wl_home']]
    
    final_df = game_base.merge(combined_features, left_on=['game_id', 'home_team_id'], right_on=['game_id', 'team_id'], suffixes=('', '_home'))
    final_df = final_df.merge(combined_features, left_on=['game_id', 'away_team_id'], right_on=['game_id', 'team_id'], suffixes=('_home', '_away'))

    # Save to a clean table for the AI
    final_df.to_sql('model_ready_features', engine, if_exists='replace', index=False)
    print(f"✅ Success! Generated 'model_ready_features' using basic box scores.")

if __name__ == "__main__":
    make_sql_table()
    calculate_basic_rolling_features()