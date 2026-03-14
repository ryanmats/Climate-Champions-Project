from datetime import time, datetime
from flask import Flask, jsonify, request, render_template
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from google.cloud import bigquery

import subprocess
import os
import pandas as pd
import numpy as np

class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, (time, datetime)):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

app = Flask(__name__, static_folder='static', template_folder='templates')
app.json = CustomJSONProvider(app)
CORS(app)

# BigQuery configuration (match etl_pipeline.py)
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', 'climate-project-489910')
DATASET_ID = "civic_data"
CLIMATE_CHAMPIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reporting_climate_champions"
PASSED_CLIMATE_BILLS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reporting_passed_climate_bills"
CLIMATE_INFLUENCE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.reporting_climate_influence_score"

# Get BigQuery client using Application Default Credentials (suitable for App Engine)
def get_bigquery_client():
    import google.auth
    credentials, project = google.auth.default()
    return bigquery.Client(credentials=credentials, project=project)


# Helper function to query BigQuery tables
def query_table(table_id, limit=1000, order_by=None):
    client = get_bigquery_client()
    sql = f"SELECT * FROM `{table_id}`"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit:
        sql += f" LIMIT {limit}"
    job = client.query(sql)
    df = job.result().to_dataframe()
    df = df.replace({float('nan'): None})
    return df


# Home route to serve the main page
@app.route('/')
def index():
    return render_template('index.html')


# API route to get climate champions data from BigQuery reporting table
@app.route('/api/climate_champions')
def api_climate_champions():
    try:
        df = query_table(CLIMATE_CHAMPIONS_TABLE, limit=500, order_by="climate_bills_passed DESC")
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# API route to get passed climate bills data from BigQuery reporting table
@app.route('/api/passed_climate_bills')
def api_passed_climate_bills():
    try:
        df = query_table(PASSED_CLIMATE_BILLS_TABLE, limit=1000, order_by='last_action_date DESC')
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# API route to get climate influencers data from BigQuery reporting table
@app.route('/api/climate_influencers')
def api_climate_influencers():
    try:
        df = query_table(CLIMATE_INFLUENCE_TABLE, limit=10, order_by="climate_influence_score DESC")
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
