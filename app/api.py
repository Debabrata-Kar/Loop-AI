from flask import Flask, request, jsonify, send_file
from app.database import Database, close_db, get_db
from app.report_generator import ReportGenerator
from app.utils import mark_report_as_complete
from config import Config
import random

app = Flask(__name__)

def teardown_db(error):
    close_db(error)

app.teardown_appcontext(teardown_db)

# Initialize a set to keep track of completed reports
completed_reports = set()

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    global database_initialized

    if not Config.DATA_LOADED_FLAG:
        db = get_db()  # Initialize the database within a request context
        db.create_tables()
        db.load_data_from_csvs()
        Config.DATA_LOADED_FLAG = True 

    report_generator = ReportGenerator(get_db())  # Initialize report generator within a request context
    report_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=10))
    # Start report generation process (to be implemented)
    report_generator.generate_report()  # Trigger report generation
    # Mark the report as complete
    mark_report_as_complete(completed_reports, report_id)

    return jsonify({'report_id': report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    # Check if report generation is complete
    if report_id in completed_reports:
        file_path = f'reports/report_{report_id}.csv'
        return send_file(file_path, as_attachment=True)
    else:
        return jsonify({'status': 'Running'})

# Assume this function is called when report generation is complete
def mark_report_as_complete(report_id):
    completed_reports.add(report_id)

if __name__ == '__main__':
    app.run(debug=True)
