import pandas as pd
from sqlalchemy import create_engine

# Path to the database we just created
engine = create_engine('sqlite:///data/nba_stats.db')

# 1. Check how many teams were loaded
teams_df = pd.read_sql("SELECT count(*) as team_count FROM teams", engine)
print(f"Total Teams: {teams_df['team_count'][0]}")

# 2. Check how many games were loaded
games_df = pd.read_sql("SELECT count(*) as game_count FROM games", engine)
print(f"Total Games: {games_df['game_count'][0]}")

# 3. Preview the last 5 games to ensure data looks 'real'
preview_df = pd.read_sql("SELECT * FROM games ORDER BY game_date DESC LIMIT 5", engine)
print("\nRecent Games Preview:")
print(preview_df[['game_id', 'game_date', 'home_pts', 'wl_home']])