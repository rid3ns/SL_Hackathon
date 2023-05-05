#snowpark
from snowflake.snowpark.session   import Session
from snowflake.snowpark.types     import IntegerType, StringType, StructField, StructType, DateType, DecimalType
from snowflake.snowpark.functions import avg, sum, col, call_udf, lit, call_builtin, year, to_decimal, split, trim
from snowflake.snowpark import dataframe 
# graphing 
import streamlit as st
import pgeocode
import altair as alt

# analytics 
import pandas as pd
import numpy  as np
import pydeck as pdk
from pydeck.types import String

#else
import sys
import datetime;
import time
from dateutil.relativedelta import relativedelta

@st.cache_data
def get_data(trigger, selected_category, selected_level, selected_state):
    hackathon_conn    = st.experimental_connection('snowpark')
    if trigger == "chart":
        
        df_DataCommonsAgg =  hackathon_conn.session.table("DATA_COMMONS_AGG_filtered")
        source            = df_DataCommonsAgg.filter(  (col('"VARIABLE_NAME"') == selected_category) 
                                                    & (col('"LEVEL"')         == selected_level)
                                                    & (col('"STATE"')         == selected_state)
                                                    ).select(col("VALUE"), col("GEO_NAME"), col("DATE")).to_pandas()      
    elif trigger == "map":
        df_DataCommonsAgg_ZIP_CODES =  hackathon_conn.session.table("DATA_COMMONS_AGG_FILTERED_ZIP_CODES")
        source                      = df_DataCommonsAgg_ZIP_CODES.filter(  (col('"VARIABLE_NAME"') == variable_value)
                                                                         & (col('"STATE"')         == selected_state)
                                                                         ).select(col("VALUE"), col("GEO_NAME"), col("DATE")).to_pandas()      
    return source


# Setting up geocoding for United States
nomi = pgeocode.Nominatim('US')

# Page Config

#Set page context
st.set_page_config(
     page_title="Census Engagement Data",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'About': "This app builds a graphic model based off values provided by snowpark and snowflake data"
     }
 )

# START PAGE TITLE # - Page Header/Subheader
st.subheader("We data profilin', we looking at that data")
# END PAGE TITLE # 

# Initialize snowpark connection. 
hackathon_conn = st.experimental_connection('snowpark')

# Start Side Panel # - This code creates the side panel which filters the overall site 
state_list        = []
level_list        = []
category_list     = []
sub_category_list = []

# tables to use in side Panel
df_LKP_State            = hackathon_conn.session.table("LKP_States")
df_LKP_Levels           = hackathon_conn.session.table("LKP_LEVELS")
df_LKP_Category         = hackathon_conn.session.table("LKP_Categories")
df_LKP_SubCategory      = hackathon_conn.session.table("LKP_SubCategories")
df_LKP_LOOKUP_ZIP_CODES = hackathon_conn.session.table("LOOKUP_ZIP_CODES")

# Create State Dataframe from df_LKP_Levels
# allow user to choose location filter type for data
state_list     = df_LKP_State.select(col("STATE")).distinct().to_pandas()   
with st.sidebar:
    with st.spinner("Loading.. "):
        selected_state = st.sidebar.selectbox('Select the state you\'d like to filter to.', df_LKP_State)

# Create Level Dataframe  from df_LKP_Levels
# allow user to choose location filter type for data
level_list     = df_LKP_Levels.select(col("LEVEL")).distinct().to_pandas()   
with st.sidebar:
    with st.spinner("Loading.. "):
        selected_level = st.sidebar.selectbox('Select the level you\'d like to filter to.', level_list)

# Create Category Dataframe from df_LKP_Category
# allow user to choose category
category_list     = df_LKP_Category.select(col("CATEGORY")).distinct().to_pandas()
with st.sidebar:
    with st.spinner("Loading.. "):
        selected_category = st.sidebar.selectbox('Select the category you\'d like to see.', category_list)

# Create subcategory Dataframe
# allow user to choose subcategory
sub_category_list   = df_LKP_SubCategory.select(col("SUB_CATEGORY")).distinct().to_pandas()
with st.sidebar:
    with st.spinner("Loading.. "):
        selected_sub_category = st.sidebar.selectbox('Select the sub-category you\'d like to see.', sub_category_list)
# END Side Panel #

if selected_sub_category is not None:
    selected_category = selected_category + ': ' + selected_sub_category

# Display selected values at the top of the screen 
col11, col12, col13, col14, col15 = st.columns(5)
with st.container():
    with col11:
        st.metric("Current Date", datetime.datetime.now().strftime("%m/%d/%Y"), delta=None, delta_color=("normal"))
    with col12:
        st.metric("State", selected_state, delta=None, delta_color=("normal"))
    with col13:           
        st.metric("Level", selected_level, delta=None, delta_color=("normal"))
    with col14:           
        st.metric("Category", selected_category, delta=None, delta_color=("normal"))
    with col15:           
        st.metric("Sub_Category", selected_sub_category, delta=None, delta_color=("normal"))

# Get dataset for charts 
df_format = get_data("chart", selected_category, selected_level, selected_state)
# START FIRST COLLAPSABLE VISUALIZATION GROUPING. # 
with st.expander(selected_category + " in " + selected_level + " " + selected_state + " Pie Visual"):
    pie_chart = alt.Chart(df_format).mark_arc().encode(
        theta=alt.Theta(field="VALUE", type="quantitative"),
        color=alt.Color(field="GEO_NAME", type="nominal"),
    )
    st.altair_chart(pie_chart, use_container_width=True)    

# start second visualization # 
with st.expander(selected_category + " in " + selected_level + " " + selected_state + " Bubble Visual"):
    chart = alt.Chart(df_format, title="Circle Chart").mark_circle().encode(
            y='VALUE',
            x='GEO_NAME',
            size='sum(VALUE):Q',
            color="GEO_NAME", 
        ).interactive()
    st.altair_chart(chart, theme="streamlit", use_container_width=True)
# END second VISUALIZATION # 

# start line visualization # 
with st.expander(selected_category + " in " + selected_level + " " + selected_state  + " Line Visual"):
    hover = alt.selection_single(
        fields=["DATE"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(df_format, title="Evolution of Value").mark_line().encode(
            x='DATE',
            y='VALUE',
            color="GEO_NAME",
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(df_format).mark_rule().encode(
            x="DATE",
            y="VALUE",
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip("DATE"    , title="Date"),
                alt.Tooltip("VALUE"   , title= selected_category + " in " + selected_level + " " + selected_state),
                alt.Tooltip("GEO_NAME", title= selected_level),
            ],
        )
        .add_selection(hover)
    )

    st.altair_chart(
        (lines + points + tooltips).interactive(),
        use_container_width=True
    )

with st.expander(selected_category + " in " + selected_level + " " + selected_state  + " map Visual"):

    # Assinging category to variable_value
    variable_value = selected_category

    # If there is a sub-category, append to category with colon in-betwee as this is how the value for the column VARIABLE_NAME is setup
    if selected_sub_category is not None:
        variable_value = variable_value + ': ' + selected_sub_category

    # Mesuring time of query to help debugging performance
    start_query_time = time.perf_counter()
    
    main_df = get_data("map", selected_category, selected_level, selected_state)
    # Assign dataframe to query response

    # Mesuring time of query to help debugging performance
    end_query_time = time.perf_counter()
    elapsed_query_time = end_query_time - start_query_time
    st.write(f'Elapsed time for query: {elapsed_query_time} seconds')

    # Create date input on sidebar based on min and max date values from dataset
    selected_date = st.sidebar.date_input(f"Date:", value=(main_df['DATE'].min(), main_df['DATE'].max()))

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
