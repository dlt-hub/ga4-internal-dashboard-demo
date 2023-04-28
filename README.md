# Google Analytics 4 (GA4) Internal Dashboard Demo

## Overview

1. We first created a `dlt` pipeline that could [load data from google analytics 4 into a local duckdb database](./intial-explorations/google_analytics_pipeline.py)

2. We then developed a [streamlit app locally with the duckdb data](./internal-dashboards/app.py)

3. After we created a pipeline and a streamlit app, we decided to move our process from local to remote for deployment

4. We created a Google Cloud VM instance and set up a postgres database in it

5. We deployed our `dlt` pipeline to [load data onto this Postgres database daily using GitHub Actions](./.github/)

6. We then finally delpoyed our streamlit app on the VM instance and modified it so that it reads data from the Postgres database inside the VM