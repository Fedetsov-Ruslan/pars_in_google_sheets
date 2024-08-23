import csv
import os
import datetime
from datetime import date
from sqlalchemy import create_engine, DateTime, Date, String, func, Integer,  ForeignKey, Enum, ARRAY, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv('DB_URL')
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
session = Session()



class Base(DeclarativeBase):
    
    created: Mapped[DateTime] = mapped_column(DateTime, default=func.now())
    updated: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class Games(Base):
    __tablename__='old_games'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    types_game: Mapped[str] = mapped_column(String(15), nullable=False)
    date_game: Mapped[date] = mapped_column(Date, nullable=False)
    gamers: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    roles: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    fols: Mapped[list[int]] = mapped_column(ARRAY(Integer), nullable=False)
    points: Mapped[list[float]] = mapped_column(ARRAY(Float), nullable=False)
    dop_points: Mapped[list[float]] = mapped_column(ARRAY(Float), nullable=True)
    best_step: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=True)
    first_dead: Mapped[str] = mapped_column(String(150), nullable=True)
    winner: Mapped[str] = mapped_column(String(15), nullable=True)

Base.metadata.drop_all(engine, [Games.__table__])

# Создание таблицы заново, если необходимо
Base.metadata.create_all(engine)

file_path = 'output.csv'

def process_table(table):
    """Обработать таблицу и извлечь нужные данные"""
    lines = table.strip().split('\n')
    
    if not lines:
        return None
    
    
    header = lines[0].split(',')
    date = header[1].split('.')
    num_game = header[5]
    date.reverse()
    date = '-'.join(date)
    player_nicks = []
    role = []
    fouls = []
    scores = []
    dop_scores = []
    
    for line in lines[2:]:
        if line.strip():
            row = line.split(',')
            if len(row) >= 6 and len(player_nicks)<10:
                player_nicks.append(row[1].strip())
                role.append(row[2].strip())
                fouls.append(row[3].strip())
                scores.append(int(row[4].strip()))
                if len (row) == 7:
                    row[6] = row[6].strip()
                    dp = '.'.join(row[5:7])
                    dp = float(dp.strip('"'))
                    dop_scores.append(dp)
                else:
                    dop_scores.append(0)

    for i in range(len(role)):
        if role[i] == '':
            role[i] = 'Мирный'
        elif role[i] == 'Ш':
            role[i] = 'Шериф'
        elif role[i] == 'Д':
            role[i] = 'Дон'
        elif role[i] == 'М':
            role[i] = 'Мафия'
        if fouls[i] == '':
            fouls[i] = 0
        else:
            fouls[i] = int(fouls[i])
    winner_team = ""
    pu_lh = []
    
    for line in lines:
        if line.startswith('Победившая команда'):
            winner_team = line.split(',')[2].strip()
        elif line.startswith('ПУ'):
            pu_lh = line.split(',')[1:]
        
    pu = pu_lh[0]
    if len(pu_lh) == 5:
        lh = [player_nicks[int(pu_lh[i])-1] for i in range(2, 5)]
    else: 
        lh = ''
    return {
        'date': date,
        'num_game': num_game,
        'player_nicks': player_nicks,
        'role': role,
        'fouls': fouls,
        'scores': scores,
        'dop_scores': dop_scores,
        'winner_team': winner_team,
        'pu': pu,
        'lh': lh,
    }

tables = []
current_table = []

with open(file_path, mode='r', encoding='utf-8-sig') as file:
    reader = csv.reader(file)
    for row in reader:
        if not any(row): 
            if current_table:
                tables.append('\n'.join(current_table))
                current_table = []
        else:
            current_table.append(','.join(row))
    if current_table:
        tables.append('\n'.join(current_table))

processed_tables = [process_table(table) for table in tables]
processed_tables.reverse()
for table in processed_tables:
    date_g = datetime.datetime.strptime(table['date'], '%Y-%m-%d').date()
    obj = Games(
        types_game = 'ranked',
        date_game = date_g,
        gamers = table['player_nicks'],
        roles = table['role'],
        fols = table['fouls'],
        points = table['scores'],
        dop_points = table['dop_scores'],
        best_step = table['lh'],
        first_dead = table['pu'],
        winner = table['winner_team'].split()[0]        
    )
    session.add(obj)
    session.commit()

