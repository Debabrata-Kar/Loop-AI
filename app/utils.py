from datetime import datetime
from pytz import timezone

completed_reports = set()

class TimezoneConverter:
    def __init__(self, source_timezone, target_timezone):
        self.source_timezone = timezone(source_timezone)
        self.target_timezone = timezone(target_timezone)

    def convert_to_target_timezone(self, timestamp_utc):
        # Convert a UTC timestamp to target timezone
        utc_time = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f UTC')
        utc_time = self.source_timezone.localize(utc_time)
        target_time = utc_time.astimezone(self.target_timezone)
        return target_time.strftime('%Y-%m-%d %H:%M:%S.%f')

def mark_report_as_complete(completed_reports, report_id):
    completed_reports.add(report_id)
