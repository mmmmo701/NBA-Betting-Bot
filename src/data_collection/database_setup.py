from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True) # NBA Official ID
    full_name = Column(String)
    abbreviation = Column(String)
    nickname = Column(String)
    city = Column(String)

class Game(Base):
    __tablename__ = 'games'
    game_id = Column(String, primary_key=True)
    game_date = Column(DateTime)
    season_id = Column(String)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    home_pts = Column(Integer)
    away_pts = Column(Integer)
    wl_home = Column(String) # 'W' or 'L'

def init_db():
    engine = create_engine('sqlite:///data/nba_stats.db')
    Base.metadata.create_all(engine)
    return engine

if __name__ == "__main__":
    init_db()
    print("✅ Database and tables created successfully.")