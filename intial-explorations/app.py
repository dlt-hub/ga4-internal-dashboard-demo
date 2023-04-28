import duckdb
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio
import plotly.express as px
import streamlit as st

from datetime import datetime, timedelta
from statsmodels.tsa.seasonal import seasonal_decompose



st.set_page_config(
    page_title="Data Attribution",
    layout="wide"
)

# Set cache to only load data once a day
@st.cache_data(ttl=60*60*24)
def get_data():

    # Load data from the locally created duckdb database
    connection = duckdb.connect("dlt_google_analytics_pipeline.duckdb")
    connection.sql("SET search_path='dlt_google_analytics_pipeline.dlt_google_analytics_data'")

    # Daily values for the metric session start
    session_start_df = connection.sql('SELECT * FROM session_start').df()

    # Daily values for the metrics page views, engaged sessions, and total sessions
    views_and_bounce_rate_df = connection.sql('SELECT * FROM views_and_bounce_rate').df()

    # Daily values for sessions by active users across the default channels  
    channel_split_df = connection.sql('SELECT * FROM channel_split').df()

    connection.close()

    return session_start_df, views_and_bounce_rate_df, channel_split_df

session_start_df, views_and_bounce_rate_df, channel_split_df = get_data()

st.header("Impact of an article on web traffic and engagement")

# Shifting the two graph filters to the right
col1, _, col3, col4 = st.columns(4)

# Setting the default date for the dashboard as the Operation Duck release date
opduck_release_date = '2023-03-09'

# Adding the option to choose date
release_date = col1.date_input(
    label="Choose date of article release",
    value=datetime.strptime(
        opduck_release_date,
        '%Y-%m-%d'
    ).date(),
    min_value=datetime.strptime(
        opduck_release_date,
        '%Y-%m-%d'
    ).date(),
    max_value=datetime.today() - timedelta(days=1)
)

# Converting the date to 'yyyy-mm-dd' string to easily input it into all functions
release_date_str = f'{release_date.year}-{release_date.month}-{release_date.day}'

# Drop down to select the metric which will be displayed in the graph
metric_option = col3.selectbox(
    "Metric",
    ("session_start","page_views","bounce_rate")
)

# We calculate data based on data one month prior to the article release
# and one month following the article release
start_date = pd.to_datetime(release_date_str) - pd.Timedelta(days=30)
end_date = pd.to_datetime(release_date_str) + pd.Timedelta(days=30)

# Creating a reference dataframe for all the dates 
# so the dates where values are missing can be merged and treated as 0
dateidx_df = pd.DataFrame(index=pd.date_range(start_date,end_date))

# Convert the date column to datetime data type
session_start_df['date'] = pd.to_datetime(
    session_start_df['date'], utc=True
).dt.date

# Fix date and trailing slashes in session_start data
session_start_df['landing_page_plus_query_string'] = \
                        session_start_df['landing_page_plus_query_string'].apply(
                            lambda x: x if x.endswith('/') else x + '/'
                        )
# Filtering the data to only include month before and month after the article release
session_start_df = session_start_df[session_start_df['date']>=start_date]\
                                    [session_start_df['date']<=end_date]

# Creating flag variable to indicate whether the date is before or after the article release
session_start_df['operation_duck'] = session_start_df['date'].apply(
    lambda date: date >= pd.to_datetime(release_date_str)
)

# Convert the date column to datetime data type
views_and_bounce_rate_df['date'] = pd.to_datetime(views_and_bounce_rate_df['date'], utc=True)

# Adding week since bounce rate bar graphs will be aggregated weekly
# Specifying day of week, since we aggregate with week starting on Thursday (release of article)
views_and_bounce_rate_df['week'] = views_and_bounce_rate_df['date'].dt.week
views_and_bounce_rate_df['day_of_week'] = views_and_bounce_rate_df['date'].dt.dayofweek
views_and_bounce_rate_df['date'] = views_and_bounce_rate_df['date'].dt.date

# Fix date and trailing slashes in the views and session engagement data
views_and_bounce_rate_df['page_path'] = views_and_bounce_rate_df['page_path'].apply(
    lambda x: x if x.endswith('/') else x + '/'
)

# Filtering the data to only include month before and month after the article release
views_and_bounce_rate_df = views_and_bounce_rate_df \
                            [views_and_bounce_rate_df['date']>=start_date] \
                            [views_and_bounce_rate_df['date']<=end_date]

# Creating flag variable to indicate whether the date is before or after the article release                            
views_and_bounce_rate_df['operation_duck'] = views_and_bounce_rate_df['date'].apply(
    lambda date: date >= pd.to_datetime(release_date_str)
)

# Convert the date column to datetime data type
channel_split_df['date'] = pd.to_datetime(channel_split_df['date'], utc=True)

# Adding week since channel split bar graphs will be aggregated weekly
# Specifying day of week, since we aggregate with week starting on Thursday (release of article)
channel_split_df['week'] = channel_split_df['date'].dt.week
channel_split_df['day_of_week'] = channel_split_df['date'].dt.dayofweek
channel_split_df['date'] = channel_split_df['date'].dt.date

# Filtering the data to only include month before and month after the article release
channel_split_df = channel_split_df[channel_split_df['date']>=start_date]\
                                    [channel_split_df['date']<=end_date]

# Creating flag variable to indicate whether the date is before or after the article release 
channel_split_df['operation_duck'] = channel_split_df['date'].apply(
    lambda date: date >= pd.to_datetime(release_date_str)
)


# Defining variables over which aggregations will be performed in each table
if metric_option == "session_start":
    metric_df = session_start_df.copy()
    metric_string = "sessions_integer"
    dimension_string = "landing_page_plus_query_string"

elif metric_option == "page_views":
    metric_df = views_and_bounce_rate_df.copy()
    metric_string = "screen_page_views_integer"
    dimension_string = "page_path"

# bounce_rate = engaged_sessions_integer / sessions_integer
# fractions can't be aggregated so the ratio is computed at each level of aggregation
elif metric_option == "bounce_rate":
    metric_df = views_and_bounce_rate_df.copy()
    metric_string = "sessions_integer"
    metric_string2 = "engaged_sessions_integer"
    dimension_string = "page_path"

# List of all webpages sorted in decreasing order of the metric value
pages_list = metric_df.groupby(dimension_string)\
                        .agg({metric_string:'sum'})\
                        .sort_values(by=metric_string,ascending=False)\
                        .index

# Take the first ten from the list of all pages
top_pages = []
count = 1
for page in pages_list:
    if count > 10:
        break
    if not page.startswith('(not set)'):
        top_pages.append(page)
        count += 1

# Drop down to select the page for which the metric graph will be displayed
# the options are going to be top pages for that metric
page_option = col4.selectbox(
    "Page",
    ["Aggregate", *top_pages]
)

if metric_option == "bounce_rate":
    # If metric 'bounce_rate' is chosen, then display the table and graph for bounce rate
    
    # Create the bounce rate table

    # Aggregate the total bounce rates for each page before and after the release of the article
    agg_df = metric_df.groupby([dimension_string,'operation_duck'])\
                      .agg({metric_string:'sum', metric_string2: 'sum'})\
                      .unstack().fillna(0) # Convert the 'operation_duck' flag to columns

    change_df = agg_df.copy() # agg_df will need to be re-used to aggregate the total value

    # Compute bounce rates for before and after the article release and get rid of the remainig columns
    change_df['Before'] = change_df[change_df.columns[2]] / change_df[change_df.columns[0]]
    change_df['After'] = change_df[change_df.columns[3]] / change_df[change_df.columns[1]]
    change_df = change_df[['Before','After']].reset_index()
    change_df.columns = ["Top pages", "Before", "After"]
    change_df = change_df[change_df["Top pages"].isin(top_pages)].reset_index(drop=True)

    # Do the same computations for all web pages combined
    total_df = pd.DataFrame(agg_df.sum()).T
    total_df['Before'] = total_df[total_df.columns[2]] / total_df[total_df.columns[0]]
    total_df['After'] = total_df[total_df.columns[3]] / total_df[total_df.columns[1]]
    total_df['Top pages'] = "Total"
    total_df = total_df[total_df.columns[-3:]]
    total_df.columns = total_df.columns.droplevel(1)

    # Concatenate the total with the values for all individual pages so that the total is at the bottom
    combined_df = pd.concat([change_df,total_df])
    combined_df['Before'] = combined_df['Before'].round(2)
    combined_df['After'] = combined_df['After'].round(2)
    combined_df = combined_df.reset_index(drop=True)

    # Changing the week start so that release of the article is treated as week start
    metric_df['modified_week'] = metric_df.apply(
        lambda row: row['week'] if row['day_of_week'] in [0,1,2] else row['week'] + 1,
        axis=1
    )

    # If 'Aggregate' is chosen in the page drop-down, keep the data as it is
    # else filter for the page option
    if page_option == "Aggregate":
        metric_plot_df = metric_df.copy()
    else:
        metric_plot_df = metric_df[metric_df[dimension_string]==page_option]

    # Creating a mapping between the week and date
    # so the graph aggregated at the week can be plotted in the datetime axis
    date_week_map = metric_df.sort_values(by='date')\
                             .drop_duplicates(subset=['modified_week'],keep='first')\
                            [['date','modified_week']]
    
    # Aggregating the data at the weekly level
    weekly_data = metric_plot_df.groupby('modified_week').agg({
                                        'screen_page_views_integer': 'sum', 
                                        'engaged_sessions_integer': 'sum'
                                    }).reset_index()
    weekly_data['bounce_rate'] = weekly_data['engaged_sessions_integer'] / weekly_data['screen_page_views_integer']
    weekly_aggregate_df = date_week_map.merge(weekly_data, on='modified_week',how='left')

    # Removing the first week for better graph plotting
    plot_df = weekly_aggregate_df[weekly_aggregate_df['modified_week'] > weekly_aggregate_df['modified_week'].min()]

    if page_option == "Aggregate":
        figure_title = f"Weekly {metric_option} site-wide"
    else:
        figure_title = f"Weekly {metric_option} for {page_option}"

    # The graph object
    fig_daily_aggregate = px.bar(
        plot_df,
        x='date', y='bounce_rate',
        title=figure_title
    )
    # Adding the the horizonal line indicating release of the article
    fig_daily_aggregate.add_vline(x=release_date_str, line_dash="dash", line_color="black")
    fig_daily_aggregate.add_annotation(
        x=release_date_str,
        y=plot_df['bounce_rate'].max(),
        text="Article release",
        yshift=-10
    )


else:
    # If metric 'session_start' or 'page_views' is chosen, 
    # then display the table and graph for it

    # Aggregate the total value for each page before and after the release of the article
    change_df = metric_df.groupby([dimension_string,'operation_duck'])\
                         .agg({metric_string:'sum'})\
                         .unstack().fillna(0) # Convert the 'operation_duck' flag to columns
    change_df.columns = change_df.columns.droplevel()
    change_df = change_df.reset_index()
    change_df.columns = ["Top pages", "Before", "After"]

    # Compute the aggregate for all pages
    total_df = pd.DataFrame(change_df.sum()).T
    total_df["Top pages"] = "Total"

    top_pages_change_df = change_df[change_df["Top pages"].isin(top_pages)]

    # Combine the value for individual pages and aggregated
    combined_df = pd.concat([top_pages_change_df,total_df])

    row_order_list = [*top_pages, "Total"] # Display pages in decreasing order of metrics with total at the end
    row_order_key = {value: idx for idx, value in enumerate(row_order_list)}

    combined_df = combined_df.sort_values(by='Top pages', key= lambda x: x.map(row_order_key))
    combined_df["Before"] = combined_df["Before"].map(int)
    combined_df["After"] = combined_df["After"].map(int)
    combined_df = combined_df.reset_index(drop=True)

    # If 'Aggregate' is chosen in the page drop-down, keep the data as it is
    # else filter for the page option
    if page_option == "Aggregate":
        metric_plot_df = metric_df.copy()
    else:
        metric_plot_df = metric_df[metric_df[dimension_string]==page_option]

    # To get a time series of daily values
    daily_aggregate_df = dateidx_df.join(
        metric_plot_df.groupby('date').agg({metric_string:'sum'}),
        how='left'
    ).fillna(0)

    # The trend line for the time series graphs
    trend_df = pd.DataFrame(
        seasonal_decompose(
            daily_aggregate_df,
            model='additive',
            two_sided=True,
            extrapolate_trend=1
        ).trend
    ).reset_index()
    trend_df.columns = ['date','trend']

    if page_option == "Aggregate":
        figure_title = f"Daily {metric_option} site-wide"
    else:
        figure_title = f"Daily {metric_option} for {page_option}"

    fig_daily_aggregate = px.line(
        daily_aggregate_df,
        title=figure_title,
        labels={
            "index": "date",
            "value": metric_option
        }
    )
    # Add trend line
    fig_daily_aggregate.add_scatter(
        x=trend_df['date'], y=trend_df['trend'],line_color='light blue'
    )
    fig_daily_aggregate.update_layout(showlegend=False)
    fig_daily_aggregate.add_vline(x=release_date_str, line_dash='dash',line_color="black")
    fig_daily_aggregate.add_annotation(
        x=release_date_str,
        y=daily_aggregate_df.values.max(),
        text="Article release",
        yshift=-10
    )


left_column, right_column = st.columns(2)

# Add table to the left
left_column.write(f"### *{metric_option}* for top pages")
left_column.table(combined_df)

# Add graph to the right
right_column.plotly_chart(fig_daily_aggregate)

# Channel split values are calculated one week before and after to capture seasonality
start_date_week = pd.to_datetime(release_date_str) - pd.Timedelta(days=7)
end_date_week = pd.to_datetime(release_date_str) + pd.Timedelta(days=7)

channels_df = channel_split_df[channel_split_df['date']>=start_date_week]\
                              [channel_split_df['date']<=end_date_week]

# Creating flag variable to indicate whether the date is before or after the article release
channels_df['operation_duck'] = channels_df['date'].apply(
    lambda date: date >= pd.to_datetime(release_date_str)
)

left_column, right_column = st.columns(2)

# Aggregated channel split the week before the article release
channels_before_df = channels_df[channels_df['operation_duck']==False]\
                                    .groupby('session_default_channel_group')\
                                    .agg({'active_users_integer':'sum'})\
                                    .reset_index()

# Aggregated channel split the week after the article release
channels_after_df = channels_df[channels_df['operation_duck']==True]\
                                    .groupby('session_default_channel_group')\
                                    .agg({'active_users_integer':'sum'})\
                                    .reset_index()

# Combining the before and after values
channels_combined_df = channels_before_df.merge(
    channels_after_df,on='session_default_channel_group',how='right'
).fillna(0)

channels_combined_df.columns = ['Channel Group', 'Week Before', 'Week After']


channels_combined_df['Week Before'] = channels_combined_df['Week Before'].map(int)
channels_combined_df['Week After'] = channels_combined_df['Week After'].map(int)


# Adding the table on the left
left_column.write('### Active users by session for each channel')
left_column.markdown('##')
left_column.table(channels_combined_df)

# Changing the week start so that release of the article is treated as week start
channel_split_df['modified_week'] = channel_split_df.apply(
    lambda row: row['week'] if row['day_of_week'] in [0,1,2] else row['week'] + 1,
    axis=1
)

# Creating a mapping between the week and date
# so the graph aggregated at the week can be plotted in the datetime axis
date_week_map = channel_split_df.sort_values(by='date')\
                                .drop_duplicates(subset=['modified_week'],keep='first')\
                                [['date','modified_week']]

weekly_data = channel_split_df.groupby(['modified_week','session_default_channel_group'])\
                              .agg({'active_users_integer':'sum'}).reset_index()

# Aggregating at the weekly level
weekly_aggregated_df = date_week_map.merge(weekly_data,on='modified_week',how='left')

plot_df = weekly_aggregated_df[
    # Removing the first week for better graph plotting
    weekly_aggregated_df['modified_week'] > weekly_aggregated_df['modified_week'].min()
]

fig_channels_bar = px.bar(
    plot_df,
    x='date',
    y='active_users_integer',
    labels={
        'active_users_integer': 'active users'
    },
    color='session_default_channel_group',
    title='Weekly active users by session default channel group'
)

# Define the height of the 'Article release' line
max_bar_height=weekly_aggregated_df.groupby('modified_week').agg({'active_users_integer':'sum'}).values.max()
fig_channels_bar.add_vline(x=release_date_str, line_dash="dash", line_color="black")
fig_channels_bar.add_annotation(
    x=release_date_str,
    y=max_bar_height,
    text="Article release",
    yshift=-10
)
# Moving the legend to the top
fig_channels_bar.update_layout(
    legend = dict(
        orientation="h",
        entrywidth=70,
        yanchor="bottom",
        y=1.0,
        xanchor="right",
        x=1.0,
        title=None
    )
)
# Add stacked bar charts to the right
right_column.plotly_chart(fig_channels_bar)
