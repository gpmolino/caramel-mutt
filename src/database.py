from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Table, Column, Sequence, MetaData, Text, Date, TIMESTAMP, Integer
from sqlalchemy.sql import insert, select
from sqlalchemy import create_engine
import uuid
import os

DATABASE_URL = os.environ['DATABASE_URL']

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://')

engine = create_engine(DATABASE_URL)

metadata_obj = MetaData()

raw_data = Table(
    'data_gathering',
    metadata_obj,
    Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("origin", Text, nullable=False),
    Column("destination", Text, nullable=False),
    Column("date", Date, nullable=False),
    Column("source", Text, nullable=False),
    Column("gathered_at", TIMESTAMP, nullable=False),
    Column("count_trips", Integer, nullable=False),
    Column("trips", Text, nullable=False),
)

city = Table(
    'cities',
    metadata_obj,
    Column("id", Integer, Sequence("city_id_seq"), primary_key=True),
    Column("name", Text, nullable=False),
    Column("state", Text, nullable=False),
    Column("alias", Text, nullable=False),
)


def save_raw_data(origin, destination, date, source, gathered_at,count_trips, trips):
    query = insert(raw_data).values(origin=origin, destination=destination, date=date, source=source, gathered_at=gathered_at,count_trips=count_trips, trips=trips)

    with engine.connect() as conn:
        result = conn.execute(query)


def get_cities():
    query = select(city)
    with engine.connect() as conn:
        result = conn.execute(query)
    return result.fetchall()
