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

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    global database_initialized

    if not Config.DATA_LOADED_FLAG:
        db = get_db()  
        db.create_tables()
        db.load_data_from_csvs()
        Config.DATA_LOADED_FLAG = True 

    report_generator = ReportGenerator(get_db()) 

    report_generator.generate_report()  
    
    completed_reports = ReportGenerator.get_completed_reports()

    return jsonify({'status': 'All reports generated.', 'completed_reports': list(completed_reports)})

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    # Get the set of completed reports
    completed_reports = ReportGenerator.get_completed_reports()

    if report_id in completed_reports:
        file_path = f'reports/report_{report_id}.csv'

        with open(file_path, 'r') as file:
            content = file.read()

            lines = content.split('\n')
            headers = lines[0].split(',')
            data = lines[1].split(',')

            report_content = {}

            for header, value in zip(headers, data):
                report_content[header] = value

            return jsonify({'status': 'Complete', 'report_content': report_content})
    else:
        return jsonify({'status': 'Running'})

if __name__ == '__main__':
    app.run(debug=True)
