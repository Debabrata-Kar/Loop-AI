from datetime import datetime, timedelta
import csv
import random
from app.database import Database
from app.utils import TimezoneConverter

import os

# Create the reports directory if it doesn't exist
os.makedirs('reports', exist_ok=True)


class ReportGenerator:
    def __init__(self, db):
        self.db = db

    def interpolate_data(self, observations, business_hours):
        total_uptime_last_hour = total_uptime_last_day = total_uptime_last_week = timedelta(0)
        total_downtime_last_hour = total_downtime_last_day = total_downtime_last_week = timedelta(0)

        # Initialize variables to keep track of the last active and inactive timestamps
        last_active_timestamp = last_inactive_timestamp = None

        for i in range(len(observations)-1):
            start_time = observations[i][0]
            end_time = observations[i+1][0]
            status = observations[i][1]

            if self.is_within_business_hours(start_time, end_time, business_hours):
                duration = end_time - start_time

                if status == 'active':
                    total_uptime_last_day += duration
                    total_uptime_last_week += duration

                    # Update last active timestamp
                    last_active_timestamp = end_time
                elif status == 'inactive':
                    total_downtime_last_day += duration
                    total_downtime_last_week += duration

                    # Update last inactive timestamp
                    last_inactive_timestamp = end_time

        # Calculate durations for the last hour
        current_time = observations[-1][0]
        if last_active_timestamp and last_active_timestamp > current_time - timedelta(hours=1):
            total_uptime_last_hour = min(current_time - last_active_timestamp, timedelta(minutes=60))

        if last_inactive_timestamp and last_inactive_timestamp > current_time - timedelta(hours=1):
            total_downtime_last_hour = min(current_time - last_inactive_timestamp, timedelta(minutes=60))

        # Convert durations to minutes
        total_uptime_last_hour = total_uptime_last_hour.total_seconds() / 60
        total_downtime_last_hour = total_downtime_last_hour.total_seconds() / 60

        # Convert other durations to hours
        total_uptime_last_day = total_uptime_last_day.total_seconds() / 3600
        total_uptime_last_week = total_uptime_last_week.total_seconds() / 3600
        total_downtime_last_day = total_downtime_last_day.total_seconds() / 3600
        total_downtime_last_week = total_downtime_last_week.total_seconds() / 3600

        return (total_uptime_last_hour, total_uptime_last_day, total_uptime_last_week,
                total_downtime_last_hour, total_downtime_last_day, total_downtime_last_week)

    # def interpolate_data(self, observations, business_hours):
    #     total_uptime_last_hour = total_uptime_last_day = total_uptime_last_week = timedelta(0)
    #     total_downtime_last_hour = total_downtime_last_day = total_downtime_last_week = timedelta(0)

    #     for i in range(len(observations)-1):
    #         start_time = observations[i][0]
    #         end_time = observations[i+1][0]
    #         status = observations[i][1]

    #         if self.is_within_business_hours(start_time, end_time, business_hours):
    #             duration = end_time - start_time

    #             if status == 'active':
    #                 total_uptime_last_day += duration
    #                 total_uptime_last_week += duration

    #                 # Calculate uptime for the last hour
    #                 if end_time > (datetime.now() - timedelta(hours=1)):
    #                     total_uptime_last_hour += min(duration, datetime.now() - end_time)

    #             elif status == 'inactive':
    #                 total_downtime_last_hour += duration
    #                 total_downtime_last_day += duration
    #                 total_downtime_last_week += duration

    #     # Ensure last hour's uptime is capped at 60 minutes and convert to minutes
    #     total_uptime_last_hour = min(total_uptime_last_hour, timedelta(minutes=60)).total_seconds() / 60

    #     return (total_uptime_last_hour, total_uptime_last_day.total_seconds() / 3600, total_uptime_last_week.total_seconds() / 3600,
    #             total_downtime_last_hour.total_seconds() / 60, total_downtime_last_day.total_seconds() / 3600, total_downtime_last_week.total_seconds() / 3600)


    def is_within_business_hours(self, start_time, end_time, business_hours):
        for (_, day, start, end) in business_hours:
            if start_time.weekday() == day and start_time.time() >= start and end_time.time() <= end:
                return True
        return False


    def generate_report(self):
        # stores = self.db.execute_query('SELECT DISTINCT store_id FROM store_status')
        stores = [54515546588432327, 8377465688456570187]
        
        # first_10_stores = stores[:10]

        # # Print the first 10 stores
        # for store_id_tuple in first_10_stores:
        #     store_id = str(store_id_tuple[0])
        #     print(store_id)

        for store_id in stores:
            # store_id = str(store_id[0]) 

            store_id = str(store_id) 

            print(store_id)
            business_hours = self.db.execute_query(f'SELECT * FROM business_hours WHERE store_id=%s', (store_id,))
            if not business_hours:
                business_hours = [(i, datetime.min.time(), datetime.max.time()) for i in range(7)]
            timezone_str = self.db.execute_query('SELECT timezone_str FROM store_timezone WHERE store_id=%s', (store_id,))
            if not timezone_str:
                timezone_str = 'America/Chicago'
            else:
                timezone_str = timezone_str[0][0]

            timezone_converter = TimezoneConverter(timezone_str, 'UTC')

            observations = self.db.execute_query('SELECT timestamp_utc, status FROM store_status WHERE store_id=%s', (store_id,))
            observations = [(row[0], row[1]) for row in observations]

            # Sort based on timestamp_utc
            observations = sorted(observations, key=lambda x: x[0])
            print(observations)

            (uptime_last_hour, uptime_last_day, uptime_last_week,
             downtime_last_hour, downtime_last_day, downtime_last_week) = self.interpolate_data(observations, business_hours)

            report_data = [
                [store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week]
            ]
            self.save_report_to_csv(store_id, report_data)

    def save_report_to_csv(self, store_id, report_data):
        report_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=10))
        file_path = f'reports/report_{report_id}.csv'

        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 
                             'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])

            for row in report_data:
                writer.writerow(row)
