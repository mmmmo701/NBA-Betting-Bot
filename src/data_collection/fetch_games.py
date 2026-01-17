import time
import pandas as pd
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamelog
from sqlalchemy.orm import sessionmaker
from database_setup import init_db, Team, Game

def populate_teams(session):
    """Fetches all 30 NBA teams and saves them to the DB."""
    nba_teams = teams.get_teams()
    for t in nba_teams:
        team_obj = Team(
            id=t['id'],
            full_name=t['full_name'],
            abbreviation=t['abbreviation'],
            nickname=t['nickname'],
            city=t['city']
        )
        session.merge(team_obj) # merge prevents duplicates
    session.commit()

def fetch_and_save_season(session, season_year):
    """Downloads all games for a specific season (e.g., '2023')."""
    print(f"--- Fetching Season {season_year}-{int(season_year)+1} ---")
    
    # LeagueGameLog gets every game for every team
    log = leaguegamelog.LeagueGameLog(
        season=season_year, 
        season_type_all_star='Regular Season'
    ).get_data_frames()[0]

    # The API gives 2 rows per game (one for each team). 
    # We consolidate this into one row (Home vs Away) for our 'games' table.
    processed_games = []
    
    # Simple logic: only grab the 'Home' side of matchups to avoid duplicates
    home_games = log[log['MATCHUP'].str.contains('vs.')]

    for _, row in home_games.iterrows():
        # Identify the Away Team from the matchup string (e.g., 'LAL vs. BOS')
        matchup = row['MATCHUP']
        away_team_abbr = matchup.split(' vs. ')[1]
        
        # We need a quick lookup to get Away Team ID from abbreviation
        # (For brevity, this assumes you've loaded teams already)
        
        game_obj = Game(
            game_id=row['GAME_ID'],
            game_date=pd.to_datetime(row['GAME_DATE']),
            season_id=row['SEASON_ID'],
            home_team_id=row['TEAM_ID'],
            home_pts=row['PTS'],
            wl_home=row['WL']
            # Note: A real production script would lookup the Away ID here too.
        )
        session.merge(game_obj)
    
    session.commit()
    time.sleep(1) # Respect Rate Limits

if __name__ == "__main__":
    engine = init_db()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    populate_teams(session)
    # Start with a small sample: the last 2 seasons
    for year in ['2023', '2024']:
        fetch_and_save_season(session, year)
    
    print("✅ Initial data load complete.")