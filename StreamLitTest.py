import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import sys
import pgeocode
import time

from pydeck.types import String
from datetime import date
from dateutil.relativedelta import relativedelta

# Setting up geocoding for United States
nomi = pgeocode.Nominatim('US')

# Page Config
st.set_page_config(page_title="Public Data Analytics", layout="wide")

# Page Header/Subheader
st.title("Public Data Analytics")
#st.subheader("We data profilin', we looking at that data")

# Initialize snowpark connection. 
hackathon_conn = st.experimental_connection('snowpark')

category_list = []
sub_category_list = []

# Create State Datafram
state_df = hackathon_conn.query(f"SELECT DISTINCT GEO_NAME as STATE FROM DATA_COMMONS_PUBLIC_DATA.CYBERSYN.GEO_INDEX WHERE LEVEL = 'State' ORDER BY GEO_NAME;")
# State Select Box
selected_state = st.sidebar.selectbox('Select the state you\'d like to see public data for.', state_df)

# Create Category Dataframe
category_df = hackathon_conn.query(f'''  
SELECT DISTINCT TRIM(SPLIT(TS.VARIABLE_NAME, ':')[0]) as CATEGORY
FROM DATA_COMMONS_PUBLIC_DATA.CYBERSYN.GEO_INDEX          GI
    JOIN DATA_COMMONS_PUBLIC_DATA.CYBERSYN.GEO_OVERLAPS   GO ON GI.ID         = GO.GEO_ID
    JOIN DATA_COMMONS_PUBLIC_DATA.CYBERSYN.TIMESERIES     TS ON GI.ID         = TS.GEO_ID
WHERE GI.GEO_NAME LIKE '%Texas%'  
ORDER BY TRIM(SPLIT(TS.VARIABLE_NAME, ':')[0])
''')

# Create Sub-Category Dataframe
sub_category_df = hackathon_conn.query(f'''  
SELECT DISTINCT TRIM(SPLIT(TS.VARIABLE_NAME, ':')[0]) as CATEGORY, TRIM(SPLIT(TS.VARIABLE_NAME, ':')[1]) as SUB_CATEGORY
FROM DATA_COMMONS_PUBLIC_DATA.CYBERSYN.GEO_INDEX          GI
    JOIN DATA_COMMONS_PUBLIC_DATA.CYBERSYN.GEO_OVERLAPS   GO ON GI.ID         = GO.GEO_ID
    JOIN DATA_COMMONS_PUBLIC_DATA.CYBERSYN.TIMESERIES     TS ON GI.ID         = TS.GEO_ID
WHERE GI.GEO_NAME LIKE '%Texas%' 
''')

# Category select box
selected_category = st.sidebar.selectbox('Select the category you\'d like to see.', category_df)

# Setting list of Sub-Categories based on chosen Category
sub_categories = sub_category_df["SUB_CATEGORY"].loc[sub_category_df['CATEGORY'] == selected_category]
# Sub-Category select box
selected_sub_category = st.sidebar.selectbox('Select the sub-category you\'d like to see.', sub_categories)

# Using expanders for each section for ease of access and smaller initial page size
with st.expander(f'Testing Dynamic SQL Based On Categories'):

    # Assinging category to variable_value
    variable_value = selected_category

    # If there is a sub-category, append to category with colon in-betwee as this is how the value for the column VARIABLE_NAME is setup
    if selected_sub_category is not None:
        variable_value = variable_value + ': ' + selected_sub_category

    # Mesuring time of query to help debugging performance
    start_query_time = time.perf_counter()

    # Dynamic sql to allow use of variables in where clause
    sql_query = f'''  

        SELECT DCA.*, LZC.CENTER_LAT, LZC.CENTER_LONG, LZC.MIN_LAT, LZC.MIN_LONG, LZC.MAX_LAT, LZC.MAX_LONG
        FROM DATA_COMMONS_AGG DCA
        JOIN LOOKUP_ZIP_CODES LZC ON DCA.ZIP_CODES = LZC.ZIP_CODES
        WHERE 1 = 1
            AND STATE           = \'''' + selected_state + '''\'
            AND VARIABLE_NAME   = \'''' + variable_value + '''\'
        ORDER BY ID, VARIABLE, DATE;

        '''
    
    # For debugging
    #st.write(sql_query)
    
    # Assign dataframe to query response
    main_df = hackathon_conn.query(sql_query)

    # Mesuring time of query to help debugging performance
    end_query_time = time.perf_counter()
    elapsed_query_time = end_query_time - start_query_time
    st.write(f'Elapsed time for query: {elapsed_query_time} seconds')

    # Create date input on sidebar based on min and max date values from dataset
    selected_date = st.sidebar.date_input(f"Date:", value=(main_df['DATE'].min(), main_df['DATE'].max()))#, min_value=min_date, max_value=max_date)

    layer_types = ['Heat Map', 'Hexagon']

    selected_layer_type = st.sidebar.selectbox('Select the layer type you would like to see on the geo graph.', layer_types)

    # If start and end date are chosen from date input, update dataframe date range
    if len(selected_date) == 2:
        selected_date_input = tuple(map(pd.to_datetime, selected_date))
        start_date, end_date = selected_date_input

    
        st.write(f'Slicing dataframe based on date...')
        # Measuring time of dataframe display
        start_query_time = time.perf_counter()

        main_df = main_df.loc[main_df['DATE'].between(start_date, end_date)]

        # Measuring time of dataframe display to help debugging performance
        end_query_time = time.perf_counter()
        elapsed_query_time = end_query_time - start_query_time
        st.write(f'Elapsed time for slicing dataframe: {elapsed_query_time} seconds')

        # Display dataframe
        st.dataframe(main_df)

    # Measuring time of dataframe display
    start_query_time = time.perf_counter()

    # Display dataframe
    st.dataframe(main_df)

    # Measuring time of dataframe display to help debugging performance
    end_query_time = time.perf_counter()
    elapsed_query_time = end_query_time - start_query_time
    st.write(f'Elapsed time for dataframe display: {elapsed_query_time} seconds')

    st.write(f'Slicing dataframe...')
    # Measuring time of dataframe display
    start_query_time = time.perf_counter()

    # Aggregate dataframe for data range
    main_df_agg = main_df.groupby(['GEO_NAME','ZIP_CODES', 'CENTER_LAT', 'CENTER_LONG']).agg({'GEO_NAME': 'max', 'ZIP_CODES': 'max', 'CENTER_LAT': 'max', 'CENTER_LONG': 'max', 'VALUE': 'mean'})

    # Assign map datafram with main aggregate dataframe lat, long, and value columns
    map_df = pd.DataFrame().assign(lat=main_df_agg['CENTER_LAT'], lon=main_df_agg['CENTER_LONG'], size=main_df_agg['VALUE'], location=main_df_agg['GEO_NAME'])

    # Measuring time of dataframe display to help debugging performance
    end_query_time = time.perf_counter()
    elapsed_query_time = end_query_time - start_query_time
    st.write(f'Elapsed time for slicing dataframe: {elapsed_query_time} seconds')

    # Display dataframe
    st.dataframe(map_df)

    view_state = pdk.data_utils.compute_view(map_df[['lon','lat']])
    view_state.pitch = 25
    view_state.zoom = 5.7

    # Measuring time of dataframe display
    start_query_time = time.perf_counter()

    # Get values for layer details
    min_size = np.min(map_df['size'], axis=0)
    max_size = np.max(map_df['size'], axis=0)

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
            'text': 'Location: {location} \r\n' + selected_category + ': {elevationValue}',
            'style': {
                'color': 'white'
            }
        }
    ))    

    # Measuring time of dataframe display to help debugging performance
    end_query_time = time.perf_counter()
    elapsed_query_time = end_query_time - start_query_time
    st.write(f'Elapsed time for graph display: {elapsed_query_time} seconds')
