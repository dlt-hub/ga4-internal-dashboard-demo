# Google Analytics 4 (GA4) Internal Dashboard Demo

## First, we used the new GA4 `dlt` pipeline and added our credentials.
## Then, we did some [initial explorations](./intial-explorations/README.md)
## Next, we created [internal dashboards](./streamlit-app/README.md) with Streamlit
## Finally, we deployed the `dlt` pipeline using GitHub Actions and the Streamlit app.

Here are the steps that we followed to X, Y, and Z:
1. We first created a `dlt` pippeline that could load the data from google analytics into a local duckdb database
2. We then developed a streamlit app locally with the duckdb data
3. After we created a pipeline and a streamlit app, we decided to move our process from local to remote for deployment
4. We created a Google Cloud VM instance, set up a postgres database in it
5. We deployed our `dlt` pipeline to load data onto this postgres database daily
6. We then finally delpoyed our streamlit app on the VM instance, and modified it so that it reads data from the posgres database inside the VM