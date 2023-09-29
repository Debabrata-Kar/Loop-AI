from datetime import datetime, timedelta
import csv
import random
from app.database import Database
from app.utils import TimezoneConverter
from app.utils import mark_report_as_complete

import os

# Create the reports directory if it doesn't exist
os.makedirs('reports', exist_ok=True)

completed_reports = set()

class ReportGenerator:
    def __init__(self, db):
        self.db = db

    def interpolate_data(self, observations, business_hours):
        total_uptime_last_hour = total_uptime_last_day = total_uptime_last_week = 0
        total_downtime_last_hour = total_downtime_last_day = total_downtime_last_week = 0

        last_active_timestamp = last_inactive_timestamp = None

        current_time = datetime(2023, 1, 25, 18, 20, 57, 676637)

        # Iterate through observations in reverse order
        for i in range(len(observations)-1, 0, -1):
            start_time = observations[i][0]
            end_time = observations[i-1][0]
            status = observations[i][1]

            if self.is_within_business_hours(start_time, end_time, business_hours):
                duration = (start_time - end_time).total_seconds() / 60  # Convert to minutes

                if status == 'active':

                    last_active_timestamp = start_time

                    if last_active_timestamp and current_time - last_active_timestamp <= timedelta(hours=1):
                        total_uptime_last_hour += min(duration, 60)  

                    if last_active_timestamp and current_time - last_active_timestamp <= timedelta(days=1):
                        total_uptime_last_day += duration

                    if last_active_timestamp and current_time - last_active_timestamp <= timedelta(weeks=1):
                        total_uptime_last_week += duration

                elif status == 'inactive':
                    last_inactive_timestamp = start_time

                    if last_inactive_timestamp and current_time - last_inactive_timestamp <= timedelta(hours=1):
                        total_downtime_last_hour += min(duration, 60) 

                    if last_inactive_timestamp and current_time - last_inactive_timestamp <= timedelta(days=1):
                        total_downtime_last_day += duration

                    if last_inactive_timestamp and current_time - last_inactive_timestamp <= timedelta(weeks=1):
                        total_downtime_last_week += duration

        return (total_uptime_last_hour, total_uptime_last_day/60, total_uptime_last_week/60,
            total_downtime_last_hour, total_downtime_last_day/60, total_downtime_last_week/60)


    def is_within_business_hours(self, start_time, end_time, business_hours):
        for (_, day, start, end) in business_hours:
            if start_time.weekday() == day and start_time.time() >= start and end_time.time() <= end:
                return True
        return False


    def generate_report(self):
        stores = self.db.execute_query('SELECT DISTINCT store_id FROM store_status')
        # stores = [54515546588432327]
        first_10 = stores[:10]

        for store_id in first_10:
        # for store_id in stores:

            store_id = str(store_id[0]) 

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

            (uptime_last_hour, uptime_last_day, uptime_last_week,
             downtime_last_hour, downtime_last_day, downtime_last_week) = self.interpolate_data(observations, business_hours)

            report_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=10))

            report_data = [
                [report_id, store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week]
            ]
            self.save_report_to_csv(store_id, report_data)
            mark_report_as_complete(completed_reports, report_id)

        print(completed_reports)

    def save_report_to_csv(self, store_id, report_data):
        report_id = report_data[0][0]
        file_path = f'reports/report_{report_id}.csv'

        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['report_id', 'store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 
                             'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])

            for row in report_data:
                writer.writerow(row)

    def get_completed_reports():
        return completed_reports
