# Initial explorations

We added our credentials and then loaded the data into a DuckDB instance on our laptop.

Here are the steps that we followed to X, Y, and Z:
1. We initiated a `dlt` project for the google analytics pipeline  
`dlt init google_analytics duckdb`
2. We added service account credentials for google analytics in `.dlt/secrets.toml`
3. We modified the script `google_analytics_pipeline.py` to specify the data loading information
    1. Using the interactive [GA4 Query Explorer](https://ga-dev-tools.google/ga4/query-explorer/) tool, we obtained the queries to request the data that we were interested in loading.
    2. We pasted these queries in the script.
    3. We specified the name of the pipeline and dataset when initializing the pipeline  
    `pipeline = dlt.pipeline(pipeline_name="dlt_google_analytics_pipeline", destination='duckdb', full_refresh=False, dataset_name="dlt_google_analytics_data")`
    4. We passed the property id and queries 
    `data_analytics = google_analytics(property_id=property_id, queries=queries) # Replace prop_id with your gooogle analytics property_id`
4. We ran the pipeline using `python3 google_analytics_pipeline.py` 
5. This resulted in the duckdb database `dlt_google_analytics_pipeline.duckdb` being created in the directory
6. We used this duckdb database to locally develop our streamlit app

Steps to run it locally:
1. Clone this repo
2. Set up environment, and set install requirements `pip install -r requirements.txt`
3. Add service account credentials for google analytics in `.dlt/secrets.toml`. On steps on how to do this using service account credentials or OAuth tokens [see here](link-to-ga4-doc)
4. Run pipeline `python3 google_analytics_pipeline.py`
5. This will create a local duckdb database `dlt_google_analytics_pipeline.duckdb`
6. Finally run the streamlit app by using the command `streamlit run app.py`
