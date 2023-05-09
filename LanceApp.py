#snowpark
from snowflake.snowpark.session   import Session
from snowflake.snowpark.types     import IntegerType, StringType, StructField, StructType, DateType, DecimalType
from snowflake.snowpark.functions import avg, sum, col, call_udf, lit, call_builtin, year, to_decimal, split, trim
from snowflake.snowpark           import dataframe 

# graphing 
import streamlit as st
import pgeocode   
import altair    as alt

# analytics 
import pandas as pd
import numpy  as np
import pydeck as pdk
from pydeck.types import String

# else
import sys
import time
import datetime
from   datetime               import date
from   dateutil.relativedelta import relativedelta

# Debugging flag. Setting up geocoding for United States
nomi      = pgeocode.Nominatim('US')
debugging = False

# Page Config + Title 
st.set_page_config(
    page_title="Census Engagement Data",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "This app builds a graphic model based off values provided by snowpark and snowflake data"
    }
 )
st.title("Public Data Analytics")

# data caching - This is a caching system holding all the queries used throughout this dashboard. This allows for a single session to run all queries needed.
@st.cache_data
def get_data(query, selected_category, selected_sub_category, selected_level, selected_state):
    # initialize snowpark connection
    hackathon_conn = st.experimental_connection('snowpark')

        # Query 1
    if query == "chart":
        # If there is a sub-category, append to category with colon in-between as this is how the value for the column VARIABLE_NAME is setup
        if selected_sub_category is not None:
            selected_category = selected_category + ': ' + selected_sub_category

        df_DataCommonsAgg = hackathon_conn.session.table("DATA_COMMONS_AGG_FILTERED_ZIP_CODES")
        source            = df_DataCommonsAgg.filter(  (col('"VARIABLE_NAME"') == selected_category) 
                                                    & (col('"LEVEL"')         == selected_level)
                                                    & (col('"STATE"')         == selected_state)
                                                    )\
                                            .select(['ZIP_CODES', 'VALUE', 'GEO_NAME', 'DATE', 'CENTER_LAT', 'CENTER_LONG', 'MIN_LAT', 'MIN_LONG', 'MAX_LAT', 'MAX_LONG']).to_pandas()
       # QUERY 2 
    elif query == "df_UNIQUE_STATE":
        df_LKP_Category    = hackathon_conn.session.table("LKP_CATEGORIES")
        source             = df_LKP_Category.select(col("STATE")).distinct().to_pandas().sort_values(by='STATE')
        
        # QUERY 3 
    elif query == "sub_categories_df":
        source    = hackathon_conn.session.table("LKP_CATEGORIES").to_pandas()
    return source

# Start Side Panel # - This code creates the side panel which filters the overall site 
state_list        = []
level_list        = []
category_list     = []
sub_category_list = []

layer_types = ['Heat Map', 'Hexagon']

# tables to use for sidebar setup. 
sub_categories_df       = get_data("sub_categories_df", "", "", "", "")
df_UNIQUE_STATE         = get_data("df_UNIQUE_STATE", "", "", "", "")

# SideBar items
selected_state          = st.sidebar.selectbox('Select state: ', df_UNIQUE_STATE)
categories              = sub_categories_df["CATEGORY"].loc[sub_categories_df['STATE'] == selected_state].drop_duplicates().sort_values()
selected_category       = st.sidebar.selectbox('Select category: ', categories)
sub_categories          = sub_categories_df["SUB_CATEGORY"].loc[sub_categories_df['CATEGORY'] == selected_category].drop_duplicates().sort_values()
selected_sub_category   = st.sidebar.selectbox('Select sub-category: ', sub_categories)
level_list              = sub_categories_df["LEVEL"].loc[(sub_categories_df['CATEGORY'] == selected_category) & ((sub_categories_df['SUB_CATEGORY'] == selected_sub_category) | (selected_sub_category is None))].drop_duplicates().sort_values()
selected_level          = st.sidebar.selectbox('Select level: ', level_list)

# Get dataset for charts 
df_format = get_data("chart", selected_category, selected_sub_category, selected_level, selected_state)

# create dataset for date 
start_date, end_date = (date.today() - relativedelta(years=3)), date.today()

# Create date input on sidebar based on min and max date values from dataset
main_df = None
if len(df_format) != 0:   
    selected_date        = st.sidebar.date_input(f"Date:", value=(df_format['DATE'].min(), df_format['DATE'].max()))

    selected_layer_type  = st.sidebar.selectbox('Select the layer type you would like to see on the geo graph.', layer_types)

    selected_date_input  = tuple(map(pd.to_datetime, selected_date))
    start_date, end_date = selected_date_input

    # If start and end date are chosen from date input, update dataframe date range
    if len(selected_date) == 2:

        selected_date_input = tuple(map(pd.to_datetime, selected_date))
        start_date, end_date = selected_date_input

        main_df = df_format.loc[df_format['DATE'].between(start_date, end_date)]

# Display selected values at the top of the screen 
col11, col12, col13, col14, col15 = st.columns(5)
with st.container():
    with col11:
        st.metric("Date Range", f'{start_date.strftime("%m/%d/%Y")} - {end_date.strftime("%m/%d/%Y")}', delta=None, delta_color=("normal"))
    with col12:
        st.metric("State", selected_state, delta=None, delta_color=("normal")) 
    with col13:           
        st.metric("Level", selected_level, delta=None, delta_color=("normal"))
    with col14:           
        st.metric("Category", selected_category, delta=None, delta_color=("normal"))
    with col15:           
        st.metric("Count",main_df.count() , delta=None, delta_color=("normal"))


selected_level_text = " "

if selected_level is not None : selected_level_text = " in " + selected_level + " "
