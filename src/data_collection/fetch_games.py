import time
import pandas as pd
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamelog
from sqlalchemy.orm import sessionmaker
from database_setup import init_db, Team, Game

def populate_teams(session):
    nba_teams = teams.get_teams()
    for t in nba_teams:
        team_obj = Team(
            id=t['id'],
            full_name=t['full_name'],
            abbreviation=t['abbreviation'],
            nickname=t['nickname'],
            city=t['city']
        )
        session.merge(team_obj)
    session.commit()
    print("Core 30 teams synchronized.")

def fetch_and_save_season(session, season_year):
    print(f"Fetching Season: {season_year}")
    
    # 1. Get every team's game log for the season
    # This returns a DataFrame where every game has TWO rows.
    log = leaguegamelog.LeagueGameLog(
        season=season_year, 
        season_type_all_star='Regular Season'
    ).get_data_frames()[0]

    # 2. Split the logs into Home and Away sets
    # In NBA Matchup strings, '@' means Away, 'vs.' means Home
    home_games = log[log['MATCHUP'].str.contains('vs.')].copy()
    away_games = log[log['MATCHUP'].str.contains('@')].copy()

    # 3. Rename columns so we can join them (Merging two structs by Game_ID)
    home_games = home_games[['GAME_ID', 'GAME_DATE', 'TEAM_ID', 'PTS', 'WL', 'SEASON_ID']]
    home_games.columns = ['GAME_ID', 'GAME_DATE', 'HOME_TEAM_ID', 'HOME_PTS', 'WL_HOME', 'SEASON_ID']
    
    away_games = away_games[['GAME_ID', 'TEAM_ID', 'PTS']]
    away_games.columns = ['GAME_ID', 'AWAY_TEAM_ID', 'AWAY_PTS']

    # 4. Join them (Like a Hash Join in 15-122)
    # We match the Home row with the Away row where GAME_ID is identical
    merged = pd.merge(home_games, away_games, on='GAME_ID')

    # 5. Iteratively save to our SQL 'struct array'
    for _, row in merged.iterrows():
        new_game = Game(
            game_id=row['GAME_ID'],
            game_date=pd.to_datetime(row['GAME_DATE']),
            season_id=row['SEASON_ID'],
            home_team_id=int(row['HOME_TEAM_ID']),
            away_team_id=int(row['AWAY_TEAM_ID']),
            home_pts=int(row['HOME_PTS']),
            away_pts=int(row['AWAY_PTS']),
            wl_home=row['WL_HOME']
        )
        session.merge(new_game)
    
    session.commit()
    print(f"Successfully saved {len(merged)} games for {season_year}.")
    time.sleep(1.5) # The "Don't get banned" timer

if __name__ == "__main__":
    # Initialize connection
    engine = init_db()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Run the process
    populate_teams(session)
    
    # Let's get the last 3 seasons for a solid training set
    seasons = ['2022', '2023', '2024']
    for s in seasons:
        fetch_and_save_season(session, s)
        
    print("\nPipeline Complete. Your database is now populated.")