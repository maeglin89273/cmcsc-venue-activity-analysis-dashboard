import pathlib
import signal
import sqlite3
from datetime import datetime, timedelta, date

import pandas as pd
from rx.subject import Subject

from data_fetcher import DataFetcher


class BusinessHours:
    def __init__(self, start_hour, end_hour):
        self.start_hour = start_hour
        self.end_hour = end_hour

    @property
    def today_start_hour_time(self):
        return self.to_exact_hour_time(datetime.now(), self.start_hour)

    @property
    def today_end_hour_time(self):
        result = self.to_exact_hour_time(datetime.now(), self.end_hour)
        if self.start_hour > self.end_hour:
            result + timedelta(days=1)

        return result


    def is_closed(self, time):
        time_hour = time.hour
        if self.start_hour < self.end_hour:
            return not self.start_hour <= time_hour < self.end_hour
        else:
            return self.end_hour <= time_hour < self.start_hour

    def estimate_how_long_to_wait_open(self, current_time):
        today_open_time = self.to_exact_hour_time(current_time, self.start_hour)
        if today_open_time >= current_time:
            return today_open_time - current_time
        else:
            return today_open_time + timedelta(days=1) - current_time

    def to_exact_hour_time(self, time, hour):
        return time.replace(hour=hour, minute=0, second=0, microsecond=0)


TABLE_NAME = 'cmcsc_room_people_number'
DAY_DATA_QUERY = "SELECT * from " + TABLE_NAME + " WHERE date(`index`) = ?"
class DataService:
    def __init__(self, sport_center_business_hours, fetch_period_seces=5, write_to_storage_period_secs=60, error_delay_secs=3):
        self.io_loop = None

        self.data_fetcher = DataFetcher()
        sqlite_file_path = pathlib.Path(__file__).parent / 'data.sqlite'
        print('database path', sqlite_file_path)
        self.sql_connection = sqlite3.connect(str(sqlite_file_path))

        self.fetch_period_secs = fetch_period_seces
        self.error_delay_secs = error_delay_secs
        self.write_to_storage_period_secs = timedelta(seconds=write_to_storage_period_secs)
        self.business_hours = sport_center_business_hours
        self.last_write_to_database_time = datetime.now()
        self.data_cache = None

        self.live_subject = Subject()


    def append_datum(self, df):
        if self.data_cache is None:
            self.data_cache = df
            return

        self.data_cache.loc[df.index[0]] = df.iloc[0]


    def write_cache_to_database(self, current_time):
        self.data_cache.to_sql(TABLE_NAME, con=self.sql_connection, if_exists='append')
        self.last_write_to_database_time = current_time
        self.data_cache = self.data_cache.iloc[0:0]
        print(datetime.now().time(), 'write into DB')

    async def fetch_callback(self):
        try:
            data = await self.data_fetcher.fetch_data()
        except Exception as e:
            print(datetime.now().time(), 'error occur, retry', e)
            self.io_loop.call_later(self.error_delay_secs, self.fetch_callback)
            return

        df = data.current_number_of_people_df
        self.append_datum(df)
        self.live_subject.on_next(df)

        if self.should_write_to_storage(data.timestamp):
            self.write_cache_to_database(data.timestamp)

        self.io_loop.call_later(self.estimate_next_fetch_delay(data.timestamp), self.fetch_callback)


    def query_last_week_data(self):
        today = date.today()
        return self.query_week_data(today - timedelta(days=1) if datetime.now() <= self.business_hours.today_end_hour_time else today)

    # from Monday
    def query_week_data(self, end_day):
        week_data = [None] * 7
        for i in range(0, 7):
            queried_date = end_day - timedelta(days=i)
            week_data[queried_date.weekday()] = self.query_datetime_indexed_db(DAY_DATA_QUERY, (queried_date,))

        return week_data


    def should_write_to_storage(self, current_time):
        return current_time - self.last_write_to_database_time >= self.write_to_storage_period_secs

    def estimate_next_fetch_delay(self, current_time):
        if self.business_hours.is_closed(current_time):
            delay = self.business_hours.estimate_how_long_to_wait_open(current_time).seconds
            print('business closed, delay: {} secs'.format(delay))
            return delay

        return self.fetch_period_secs


    def subscribe(self, doc, on_next, on_error):
        disposable = self.live_subject.subscribe(on_next=on_next, on_error=on_error)
        def on_session_destroyed(session_context):
            disposable.dispose()
            print('observer disposed, observers: ', len(self.live_subject.observers))

        doc.on_session_destroyed(on_session_destroyed)
        today_df = self.query_datetime_indexed_db(DAY_DATA_QUERY, (date.today(),))

        return today_df.append(self.data_cache)

    def query_datetime_indexed_db(self, query_str, params=None):
        return pd.read_sql_query(query_str,
                          self.sql_connection,
                          params=params,
                          index_col='index',
                          parse_dates=['index'])

    def start(self, io_loop):
        self.io_loop = io_loop

        self.register_quit_signals()
        self.io_loop.call_later(self.estimate_next_fetch_delay(datetime.now()), self.fetch_callback)

    def register_quit_signals(self):
        # signal.signal(signal.SIGQUIT, self.shutdown)  # SIGQUIT is send by our supervisord to stop this server.
        signal.signal(signal.SIGABRT, self.shutdown)  # SIGQUIT is send by our supervisord to stop this server.
        signal.signal(signal.SIGTERM, self.shutdown)  # SIGTERM is send by Ctrl+C or supervisord's default.
        signal.signal(signal.SIGINT, self.shutdown)


    def shutdown(self):
        self.write_cache_to_database(datetime.now())
        print('data service shutdown')
        self.sql_connection.close()
