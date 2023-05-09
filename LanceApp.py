#snowpark
from snowflake.snowpark.session   import Session
from snowflake.snowpark.types     import IntegerType, StringType, StructField, StructType, DateType, DecimalType
from snowflake.snowpark.functions import avg, sum, col, call_udf, lit, call_builtin, year, to_decimal, split, trim
from snowflake.snowpark           import dataframe 

# graphing 
import streamlit as st
import pgeocode   
import altair    as alt
from PIL import Image
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
import base64

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

# LINK TO THE CSS FILE
with open('style.css')as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html = True)

def add_bg_from_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url(data:image/{"png"};base64,{encoded_string.decode()});
        background-size: cover
    }}
    </style>
    """,
    unsafe_allow_html=True
    )

add_bg_from_local('.\img\ArmetaHomepage.png')  
# Title

# data caching - This is a caching system holding all the queries used throughout this dashboard. This allows for a single session to run all queries needed.
@st.cache_data
def get_data(query, selected_category, selected_level, selected_state):
    # initialize snowpark connection
    hackathon_conn = st.experimental_connection('snowpark')

        # Query 1
    if query == "chart":
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
sub_categories_df       = get_data("sub_categories_df", "", "", "")
df_UNIQUE_STATE         = get_data("df_UNIQUE_STATE", "", "", "")

# SideBar items
selected_state          = st.sidebar.selectbox('Select state: ', df_UNIQUE_STATE)
categories              = sub_categories_df["CATEGORY"].loc[sub_categories_df['STATE'] == selected_state].drop_duplicates().sort_values()
selected_category       = st.sidebar.selectbox('Select category: ', categories)
sub_categories          = sub_categories_df["SUB_CATEGORY"].loc[sub_categories_df['CATEGORY'] == selected_category].drop_duplicates().sort_values()
selected_sub_category   = st.sidebar.selectbox('Select sub-category: ', sub_categories)
level_list              = sub_categories_df["LEVEL"].loc[(sub_categories_df['CATEGORY'] == selected_category) & ((sub_categories_df['SUB_CATEGORY'] == selected_sub_category) | (selected_sub_category is None))].drop_duplicates().sort_values()
selected_level          = st.sidebar.selectbox('Select level: ', level_list)

# If there is a sub-category, append to category with colon in-between as this is how the value for the column VARIABLE_NAME is setup
if selected_sub_category is not None:
    selected_category = selected_category + ': ' + selected_sub_category

# Get dataset for charts 
df_format = get_data("chart", selected_category, selected_level, selected_state)

# create dataset for date 
start_date, end_date = date.today() - relativedelta(years=3), date.today()

# Create date input on sidebar based on min and max date values from dataset
main_df = None
if len(df_format) != 0:   
    selected_date        = st.sidebar.date_input(f"Date:", value=(df_format['DATE'].min(), df_format['DATE'].max()))
    selected_layer_type  = st.sidebar.selectbox('Select layer type for geo graph.', layer_types)
    selected_date_input  = tuple(map(pd.to_datetime, selected_date))
    start_date, end_date = selected_date_input

    # If start and end date are chosen from date input, update dataframe date range
    if len(selected_date) == 2:
        selected_date_input  = tuple(map(pd.to_datetime, selected_date))
        start_date, end_date = selected_date_input
        main_df              = df_format.loc[df_format['DATE'].between(start_date, end_date)]

st.markdown("<h1 style='text-align: center; color: white;'>Public Data Analytics for date range: "+f'{start_date.strftime("%m/%d/%Y")} - {end_date.strftime("%m/%d/%Y")}'+"</h1>", unsafe_allow_html=True)

# Display selected values at the top of the screen 
col11, col12, col13 = st.columns(3)
with st.container():
    with col11:
        st.metric("State", "Selected State: " + selected_state, delta=None, delta_color=("normal"), label_visibility="hidden") 
    with col12:           
        st.metric("Level", "Selected Level: " + selected_level, delta=None, delta_color=("normal"), label_visibility="hidden")
    with col13:           
        st.metric("Category", "Selected Metric: " + selected_category, delta=None, delta_color=("normal"), label_visibility="hidden")

selected_level_text = " "

if selected_level is not None : selected_level_text = " in " + selected_level + " "
#####################################################################
#   BEGIN - GEO-VISUAL SECTION
#####################################################################

with st.expander(selected_category + selected_level_text + selected_state + " Geo Visual"):

    if main_df is not None:

        # Aggregate dataframe for data range
        main_df_agg = main_df.groupby(['GEO_NAME','ZIP_CODES', 'CENTER_LAT', 'CENTER_LONG']).agg({'GEO_NAME': 'max', 'ZIP_CODES': 'max', 'CENTER_LAT': 'max', 'CENTER_LONG': 'max', 'VALUE': 'mean'})

        # Assign map datafram with main aggregate dataframe lat, long, and value columns
        map_df = pd.DataFrame().assign(lat=main_df_agg['CENTER_LAT'], lon=main_df_agg['CENTER_LONG'], size=main_df_agg['VALUE'], location=main_df_agg['GEO_NAME'])

        # Display dataframe
        if debugging:
            st.dataframe(map_df)

        view_state = pdk.data_utils.compute_view(map_df[['lon','lat']])
        view_state.pitch = 25
        view_state.zoom = 5.7

        # Get values for layer details
        min_size = np.min(map_df['size'], axis=0)
        max_size = np.max(map_df['size'], axis=0)

        if debugging:
            st.write(f'Min Size: {min_size} | Max Size: {max_size} | Min/Max Ratio: {min_size/max_size} | Max/Min Ratio: {max_size/min_size}')

        geo_layer = None

        if selected_layer_type == 'Hexagon':
            geo_layer = pdk.Layer(
                'HexagonLayer',
                data=map_df,
                get_position='[lon, lat]',
                radius=10000,
                elevation_scale=min_size,
                pickable=True,
                extruded=True,
                getElevationWeight="size",
                coverage=1,
                location="location",
            ),
        elif selected_layer_type == 'Blah':
            geo_layer = pdk.Layer(
                'HexagonLayer',
                data=map_df,
                get_position='[lon, lat]',
                radius=500,
                elevation_scale=25,
                elevation_range=[0, 10000],
                pickable=True,
                extruded=True,
                coverage=1,
            ),
        else:
            geo_layer = pdk.Layer(
                "HeatmapLayer",
                data=map_df,
                opacity=0.9,
                get_position=["lon", "lat"],
                getWeight='size',
                location="location",
            ),


        st.pydeck_chart(pdk.Deck(
            map_style='dark',
            #height=st.screen_height * 0.5,
            initial_view_state=view_state,
            #pdk.ViewState(
            #    latitude=30.9433703,
            #    longitude=-99.7004626,
            #    zoom=5.7,
            #    pitch=25,
            #),
            layers=[
                geo_layer
            ],
            tooltip={
                #'html': '<b>' + selected_category + ':</b> {elevationValue}<br><b>Location:</b> {location}<br> ',
                'text': selected_category + ': {elevationValue}',
                'style': {
                    'color': 'white',
                    'font-size': '40px'
                }
            }
        ))    

#####################################################################
#   END - GEO-VISUAL SECTION
#####################################################################