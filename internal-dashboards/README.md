# Internal dashboards

After the initial explorations, we created our internal dashboards as a Streamlit app.

Here are the steps that we followed to X, Y, and Z:
1. We first developed the streamlit app locally:
    1. We started by loading the data from the local duckdb database containing the google analytics data created in the previous step
    2. Once the data was loaded, it was easy to develop the dashboard in streamlit just by using pandas and plotly.
    3. To access the dashboard, we just had to run `streamlit run app.py` where `app.py` was the name of our streamlit app.
2. After we developed the app locally, the next step was deploying it on a remote server so that everyone in the company could access it.
3. This also meant that we had to write the data in a location that would be accessible to the streamlit app in the remote server.
4. Hence we decided to delpoy the streamlit app on a Google Cloud VM instance, and load the data into a Postgres database in the same VM instance.
5. The main steps that we took for this were as follows:
    1. We created a google VM instance, installed PostgreSQL on it, and configured postgres and the VM so that our `dlt` pipeline could load data onto it
    2. We used `dlt` to load the google analytics data into this postgres database
        1. We created a new dlt project with postgres as a destination  
        `dlt init google_analytics postgres`
        2. In addition to the google analytics credentials, we also added credentials for the remote postgres database to `.dlt/secrets.toml`
        3. We made the same changes to `google_analytics_pipeline.py` and ran `python google_analytics_pipeline.py`
        4. This loaded the google analytics data onto the postgres database
    4. We pushed the streamlit app to the VM and configured the VM to allow incoming traffic to the port where we were going to run streamlit
    5. We installed all dependencies needed to run the streamlit app
    6. We then modified the streamlit app to read data from the postgres database instead of the duckdb database, and finally ran it using the same command `streamlit run app.py"