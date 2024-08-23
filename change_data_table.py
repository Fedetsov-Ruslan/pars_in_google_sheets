import os

from sqlalchemy import DateTime,Date, String, func, Integer,  ForeignKey, Enum, ARRAY, Float, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from datetime import date
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv('DB_URL')
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
session = Session()


class Base(DeclarativeBase):    
    created: Mapped[DateTime] = mapped_column(DateTime, default=func.now())
    updated: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class OldGames(Base):
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


class Users(Base):
    __tablename__='users'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    nickname: Mapped[str] = mapped_column(String(150), nullable=False)
    gender: Mapped[str] = mapped_column(String(15), nullable=True)
    club: Mapped[str] = mapped_column(String(150), nullable=True)

class Games(Base):
    __tablename__='games'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    type_games: Mapped[str] = mapped_column(String(15), nullable=False)
    date_game: Mapped[date] = mapped_column(Date, nullable=False)
    first_dead: Mapped[str] = mapped_column(String(150), nullable=True)
    winner: Mapped[str] = mapped_column(String(15), nullable=True)


class GameResults(Base):
    __tablename__='game_results'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey('games.id'), nullable=False)
    player_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False)
    seat_number: Mapped[int] = mapped_column(Integer, nullable=False)
    role: Mapped[str] = mapped_column(String(15), nullable=False)
    fols: Mapped[int] = mapped_column(Integer, nullable=False)
    points: Mapped[float] = mapped_column(Float, nullable=False)
    dop_points: Mapped[float] = mapped_column(Float, nullable=True)


class BestStep(Base):
    __tablename__ = 'best_step'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey('games.id'), nullable=False)
    seat_number: Mapped[int] = mapped_column(Integer, nullable=False)


# Base.metadata.create_all(engine)

for old_game in session.query(OldGames).order_by(OldGames.id).all():
    # Создаем новую запись в Games
    new_game = Games(
        type_games=old_game.types_game,
        date_game=old_game.date_game,
        first_dead=old_game.first_dead,
        winner=old_game.winner
    )
    session.add(new_game)
    session.flush()  # Чтобы получить сгенерированный id

    # Создаем записи в GameResults
    for idx, player in enumerate(old_game.gamers):
        person = session.execute(select(Users).filter_by(nickname=player)).first() 
        if person is None:
            new_user = Users(
                nickname=player,
                gender= None,
                club='Red Move'
            )
            session.add(new_user)
            
        user = session.query(Users).filter_by(nickname=player).first()
        game_result = GameResults(
            game_id=new_game.id,
            player_id=user.id,  # Функция для получения id игрока
            seat_number=idx + 1,  # Номер места
            role=old_game.roles[idx],
            fols=old_game.fols[idx],
            points=old_game.points[idx],
            dop_points=old_game.dop_points[idx] if old_game.dop_points else None
        )
        session.add(game_result)

    # Создаем записи в BestSteps
    for seat in old_game.best_step:
        best_step = BestStep(
            game_id=new_game.id,
            seat_number=user.id
        )
        session.add(best_step)

session.commit()