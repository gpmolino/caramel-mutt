from prefect import task, Flow
from datetime import timedelta, date
from prefect.schedules import IntervalSchedule
import scrapper_clickbus as cb
import database as db
import os

FUTURE_DAYS = os.environ['FUTURE_DAYS']
TIME_INTERVAL_MINUTES = os.environ['TIME_INTERVAL_MINUTES']


@task
def extract(future_days):
    gathered_data = []
    cities = db.get_cities()

    for origin in cities:
        for destination in cities:
            for increment_days in range(future_days):
                ref_date = str(date.today() + timedelta(days=increment_days))
                if origin != destination:
                    unit = cb.scrapper_trips(origin=origin['alias'], destination=destination['alias'], date=ref_date)
                    gathered_data.append(unit)

    return gathered_data


@task
def load(data_collection):
    for record in data_collection:
        if record['count_trips'] > 0:
            db.save_raw_data(origin=record['origin'], destination=record['destination'], date=record['date'], source=record['source'], gathered_at=record['gathered_at'], count_trips=record['count_trips'], trips=record['trips'])


schedule = IntervalSchedule(interval=timedelta(hours=TIME_INTERVAL_MINUTES))


with Flow("click-bus-flow", schedule=schedule) as flow:
    data = extract(FUTURE_DAYS)
    load(data)

flow.run()
