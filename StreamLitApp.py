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

# data caching
@st.cache_data
def get_data(query, selected_category, selected_sub_category, selected_level, selected_state):
    hackathon_conn = st.experimental_connection('snowpark')
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
    elif query == "df_UNIQUE_STATE":
        df_LKP_Category    = hackathon_conn.session.table("LKP_CATEGORIES")
        source    = df_LKP_Category.select(col("STATE")).distinct().to_pandas().sort_values(by='STATE')
    elif query == "sub_categories_df":
        
    return source


# Initialize snowpark connection. 
hackathon_conn = st.experimental_connection('snowpark')

# Start Side Panel # - This code creates the side panel which filters the overall site 
state_list        = []
level_list        = []
category_list     = []
sub_category_list = []

layer_types = ['Heat Map', 'Hexagon']

# tables to use in side Panel

# Debug Checkbox
# debugging = st.sidebar.checkbox('Debugging')
sub_categories_df       = df_LKP_Category.to_pandas()

# SideBar items
selected_state          = st.sidebar.selectbox('Select the state you\'d like to see public data for.', df_UNIQUE_STATE)

categories              = sub_categories_df["CATEGORY"].loc[sub_categories_df['STATE'] == selected_state].drop_duplicates().sort_values()
selected_category       = st.sidebar.selectbox('Select the category you\'d like to see.', categories)

sub_categories          = sub_categories_df["SUB_CATEGORY"].loc[sub_categories_df['CATEGORY'] == selected_category].drop_duplicates().sort_values()
selected_sub_category   = st.sidebar.selectbox('Select the sub-category you\'d like to see.', sub_categories)

level_list              = sub_categories_df["LEVEL"].loc[(sub_categories_df['CATEGORY'] == selected_category) & ((sub_categories_df['SUB_CATEGORY'] == selected_sub_category) | (selected_sub_category is None))].drop_duplicates().sort_values()
selected_level          = st.sidebar.selectbox('Select the level you\'d like to filter to.', level_list)

# Get Data
# main_df = get_data_V2(selected_category, selected_sub_category, selected_level, selected_state)
# Get dataset for charts 
df_format = get_data(selected_category, selected_sub_category, selected_level, selected_state)

start_date, end_date = date.today() - relativedelta(years=3), date.today()

main_df = None
if len(df_format) != 0:
    # Create date input on sidebar based on min and max date values from dataset
    selected_date       = st.sidebar.date_input(f"Date:", value=(df_format['DATE'].min(), df_format['DATE'].max()))
    selected_layer_type = st.sidebar.selectbox('Select the layer type you would like to see on the geo graph.', layer_types)

    selected_date_input = tuple(map(pd.to_datetime, selected_date))
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
        st.metric("Sub-Category", selected_sub_category, delta=None, delta_color=("normal"))


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
                    'color': 'white'
                }
            }
        ))    

#####################################################################
#   END - GEO-VISUAL SECTION
#####################################################################


#####################################################################
#   BEGIN - CHART SECTION
#####################################################################


# START FIRST COLLAPSABLE VISUALIZATION GROUPING. # 
#with st.expander(selected_category + " in " + selected_level + " " + selected_state + " Pie Visual"):
#    pie_chart = alt.Chart(df_format).mark_arc().encode(
#        theta=alt.Theta(field="VALUE", type="quantitative"),
#        color=alt.Color(field="GEO_NAME", type="nominal"),
#    )
#    st.altair_chart(pie_chart, use_container_width=True)    

# start second visualization # 

with st.expander(selected_category + selected_level_text + selected_state + " Bubble Visual"):
    chart = alt.Chart(df_format, title="Circle Chart").mark_circle().encode(
            y='VALUE',
            x='GEO_NAME',
            size='sum(VALUE):Q',
            color="GEO_NAME", 
        ).interactive()
    st.altair_chart(chart, theme="streamlit", use_container_width=True)
# END second VISUALIZATION # 

# start line visualization # 
with st.expander(selected_category + selected_level_text + selected_state  + " Line Visual"):
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
                alt.Tooltip("VALUE"   , title= selected_category + selected_level_text + selected_state),
                alt.Tooltip("GEO_NAME", title= selected_level),
            ],
        )
        .add_selection(hover)
    )

    st.altair_chart(
        (lines + points + tooltips).interactive(),
        use_container_width=True
    )


#####################################################################
#   END - CHART SECTION
#####################################################################