import requests
import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Local CSV File paths
DEFAULT_PATH = 'data/our_data_1.csv'

# API Keys
OPENSTATES_API_KEY = 'INSERT_API_KEY_HERE' # Get your own API key from https://openstates.org/api/v3/#section/Authentication
LEGISCAN_API_KEY = 'INSERT_API_KEY_HERE' # Get your own API key from https://legiscan.com/gaits/register
BILLTRACK50_API_KEY = 'INSERT_API_KEY_HERE' # Get your own API key from https://www.billtrack50.com/signup

# MAX LIMIT
MAX_LIMIT=400

# Google Cloud Configuration
PROJECT_ID = "climate-project-489910"
DATASET_ID = "civic_data"
GOOGLE_CLOUD_CREDENTIALS_PATH = 'service.json'

# Source tables
LEGISCAN_MASTERLIST_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_legiscan_masterlist"
OPENSTATES_LEGISLATORS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_openstates_ca_legislators"

# Staging tables
PASSED_CLIMATE_BILLS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.stg_passed_climate_bills"
PASSED_CLIMATE_BILLS_SPONSORS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.stg_passed_climate_bills_sponsors"
PASSED_CLIMATE_BILLS_AISUMMARIES_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.stg_passed_climate_bills_aisummaries"

# Reporting tables
REPORTING_CLIMATE_CHAMPIONS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.reporting_climate_champions"
REPORTING_PASSED_CLIMATE_BILLS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.reporting_passed_climate_bills"


# Takes a DataFrame and uploads it to BigQuery at a given table ID. It replaces the table if it already exists.
def upload_to_bigquery(df, table_id):
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_CLOUD_CREDENTIALS_PATH)   
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE replaces the table each time
        write_disposition="WRITE_TRUNCATE", 
        autodetect=True,
    )
    print(f"Uploading {len(df)} rows to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Waits for the job to complete
    print(f"Successfully loaded data to {table_id}.")


# Creates a BigQuery table given a SQL statement and table ID
def create_bigquery_table(sql):
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_CLOUD_CREDENTIALS_PATH)   
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    query_job = client.query(sql)  # API request
    query_job.result()  # Waits for the job to complete
    print(f"BigQuery table created successfully.")


# Get the results of a BigQuery SQL query and return as a DataFrame
def get_bigquery_query_results(sql):
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_CLOUD_CREDENTIALS_PATH)   
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    query_job = client.query(sql) 
    df = query_job.to_dataframe()
    print(f"Successfully retrieved {len(df)} rows from BigQuery.")
    return df

# Source Table: Legiscan Masterlist for California (ID 2172)
# This is a comprehensive list of all bills, including metadata and descriptions,
# which we will filter down to climate-related bills in our staging table.
def get_legiscan_masterlist_data():
    base_url = f"https://api.legiscan.com/?key={LEGISCAN_API_KEY}"
    params = {
        "op": "getMasterList",
        "id": 2172
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status() # Check if the request was successful
        data = response.json()
        results = data.get("masterlist", [])
        df = pd.DataFrame.from_dict(data['masterlist'], orient='index')
        # Reset the index if you don't want the "0", "1" strings as your index
        df.reset_index(drop=True, inplace=True)
        # Drop rows where 'bill_id' is null
        df = df.dropna(subset=['bill_id'])
        # List of metadata columns to remove
        metadata_cols = [
            'session_id', 'state_id', 'year_start', 'year_end', 
            'prefile', 'sine_die', 'prior', 'special', 
            'session_tag', 'session_title', 'session_name'
        ]
        # Drop the metadata columns
        df = df.drop(columns=metadata_cols)
        upload_to_bigquery(df, LEGISCAN_MASTERLIST_TABLE_ID)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")


# Source Table: Open States California Legislators
# This is a nightly-updated CSV of all current legislators for California from Open States.
def get_openstates_california_legislators_data():
    df_legislators = pd.read_csv("https://data.openstates.org/people/current/ca.csv")
    upload_to_bigquery(df_legislators, OPENSTATES_LEGISLATORS_TABLE_ID)


# Staging Table: Passed Climate Bills
# This table filters the Legiscan Masterlist down to bills that are climate-related and have been passed (status = 4.0 in Legiscan).
# Simple keyword search on title and description to identify climate-related bills. We can iterate and improve this logic over time.
def get_passed_climate_bills_data():
    passed_climate_bills_sql = f"""
    CREATE OR REPLACE TABLE {PASSED_CLIMATE_BILLS_TABLE_ID} AS
    SELECT 'CA' as state, number as bill_number, title, status, last_action  FROM {LEGISCAN_MASTERLIST_TABLE_ID} WHERE
  (REGEXP_CONTAINS(title, r'(climate|environment|emission|energy|pollution|greenhouse)') OR REGEXP_CONTAINS(description, r'(climate|environment|emission|energy|pollution|greenhouse)'))
    AND status = 4.0;
    """
    create_bigquery_table(passed_climate_bills_sql)


# Staging Table: Passed Climate Bills Sponsors
# This table takes the list of passed climate bills and gets all their sponsors from the Open States API.
def get_passed_climate_bills_sponsors_data():
    # 1. Get bill numbers from BigQuery
    sql = f"SELECT bill_number FROM {PASSED_CLIMATE_BILLS_TABLE_ID}"
    df = get_bigquery_query_results(sql)
    
    # Remove spaces from bill numbers for the Open States v3 API identifier match
    bill_list = df['bill_number'].str.replace(" ", "", regex=False).tolist()
    
    base_url = "https://v3.openstates.org/bills"
    headers = {"X-API-KEY": OPENSTATES_API_KEY}

    # Collect all sponsors in a list of dictionaries, which we will convert to a DataFrame at the end.
    all_sponsors = []

    # 2. Break the list into chunks of 10 (to respect Open States API limits) and loop through each chunk
    chunk_size = 5 
    for i in range(0, len(bill_list), chunk_size):
        batch = bill_list[i : i + chunk_size]
        
        params = {
            "jurisdiction": "California",
            "session": "20252026", # Fixed hyphenated session
            "identifier": batch, # Requests handles the list expansion
            "include": "sponsorships" # Get sponsorships data
        }

        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # 3. Process the results in this batch
            for bill_data in data.get("results", []):
                print(bill_data.get("sponsorships"))
                for sponsor in bill_data.get("sponsorships", []):

                    # Get relevant sponsor info for a bill and append to our list of dictionaries
                    all_sponsors.append({
                        "bill_number": bill_data['identifier'],
                        "ocd_bill_id": bill_data['id'],
                        "ocd_person_id": sponsor.get("person").get("id"),
                        "sponsor_name": sponsor.get("person").get("name"),
                        "party": sponsor.get("person").get("party"),
                        "role": sponsor.get("person").get("current_role").get("title"),
                        "district": sponsor.get("person").get("current_role").get("district"),
                        "is_primary": sponsor.get("primary", False)
                    })
            
            print(f"Processed batch {i//chunk_size + 1}...")
            
            # 4. Small delay to be polite to the API and avoid hitting rate limits (Open States allows 10 req/sec, we are doing 1 req per chunk of 5 bills, so this is very safe)
            time.sleep(1)

        except Exception as e:
            print(f"Error in batch starting at index {i}: {e}")

    print(all_sponsors)
    # Convert the list of dictionaries to a DataFrame
    df_sponsors = pd.DataFrame(all_sponsors)
    # Upload DataFrame to BigQuery
    upload_to_bigquery(df_sponsors, PASSED_CLIMATE_BILLS_SPONSORS_TABLE_ID)


# Staging Table: Passed Climate Bills AI Summaries
# This table takes the list of passed climate bills and gets their AI-generated summaries from the BillTrack50 API.
# We use the 'searchText' parameter to find the bill by its number
def get_billtrack50_aisummaries():

    # Get bill numbers of passed climate bills from BigQuery Passed Climate Bills staging table
    sql = f"SELECT bill_number FROM {PASSED_CLIMATE_BILLS_TABLE_ID}"
    passed_climate_bills_df = get_bigquery_query_results(sql)
    passed_climate_bills_list = passed_climate_bills_df['bill_number']

    # Define headers (recommended way to pass the API key)
    headers = {
        "Authorization": f"apikey {BILLTRACK50_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Store all summaries in a list of dictionaries, which we will convert to a DataFrame at the end and upload to BigQuery
    all_summaries = []

    # Loop through each bill number and make a BillTrack50 API call to get its AI summary
    for bill_number in passed_climate_bills_list:
        # Use 'searchText' for the specific bill number
        # StateCodes is vital to avoid duplicates from other years/states
        params = {
            "searchText": bill_number,
            "stateCodes": "CA"
        }
        
        response = requests.get(
            "https://www.billtrack50.com/BT50Api/2.1/json/bills",
            headers={"Authorization": f"apikey {BILLTRACK50_API_KEY}"},
            params=params
        )
        
        if response.status_code == 200:
            data = response.json().get("bills", [])
            if data:
                # The first result is usually the exact match
                bill = data[0]
                all_summaries.append({
                    "bill_number": bill_number,
                    "ai_summary": bill.get("aiSummary")
                })
                print(f"✅ Retrieved summary for {bill_number}")
            else:
                print(f"⚠️ No result found for {bill_number}")
                
        # Respect the 5 req/sec limit
        time.sleep(0.25)

    # Convert to final DataFrame
    df_summaries = pd.DataFrame(all_summaries)

    # Create BigQuery Staging Table
    upload_to_bigquery(df_summaries, PASSED_CLIMATE_BILLS_AISUMMARIES_TABLE_ID)


# Reporting Table: Climate Champions
# This table identifies the top 10 climate champions based on the number of passed climate bills they sponsored,
# and enriches that data with legislator information from Open States.
def create_reporting_table_climate_champions():
    reporting_table_climate_champions_sql = f"""
    CREATE OR REPLACE TABLE {REPORTING_CLIMATE_CHAMPIONS_TABLE_ID} AS (
        WITH top10_climate_champions AS (
            SELECT
                sponsor_name,
                party,
                COUNT(*) AS climate_bills_passed
            FROM `{PASSED_CLIMATE_BILLS_SPONSORS_TABLE_ID}`
            GROUP BY sponsor_name, party
            ORDER BY climate_bills_passed DESC
            LIMIT 10
        )
        
        SELECT
            top10.sponsor_name,
            top10.party,
            top10.climate_bills_passed,
            legislators.current_district AS district,
            legislators.current_chamber AS chamber,
            legislators.gender,
            legislators.email,
            legislators.birth_date,
            legislators.image,
            legislators.district_address,
            legislators.district_voice
        FROM top10_climate_champions AS top10
        LEFT JOIN `{OPENSTATES_LEGISLATORS_TABLE_ID}` AS legislators
        ON top10.sponsor_name = legislators.name
    );
    """
    create_bigquery_table(reporting_table_climate_champions_sql)


# Reporting Table: Passed Climate Bills with AI Summaries
# This table combines the passed climate bills with their AI summaries for easy querying and display in the frontend.
def create_reporting_table_passed_climate_bills():
    reporting_table_passed_climate_bills_sql = f"""
    CREATE OR REPLACE TABLE {REPORTING_PASSED_CLIMATE_BILLS_TABLE_ID} AS (
        SELECT
            b.bill_number,
            b.title,
            a.ai_summary
        FROM `{PASSED_CLIMATE_BILLS_TABLE_ID}` AS b
        LEFT JOIN `{PASSED_CLIMATE_BILLS_AISUMMARIES_TABLE_ID}` AS a
        ON b.bill_number = a.bill_number
    );
    """
    create_bigquery_table(reporting_table_passed_climate_bills_sql)


# Run all the functions in sequence to populate our BigQuery tables with source data,
# then transform that data in staging tables, and finally create our reporting tables.
# This is the main function we will run on a schedule (e.g. daily) to keep our data up to date.
if __name__ == "__main__":

    # Source tables
    get_legiscan_masterlist_data()
    get_openstates_california_legislators_data()

    # Staging tables
    get_passed_climate_bills_data()
    get_passed_climate_bills_sponsors_data()
    get_billtrack50_aisummaries()

    # Reporting tables
    create_reporting_table_climate_champions()
    create_reporting_table_passed_climate_bills()