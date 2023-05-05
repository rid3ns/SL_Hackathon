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

@st.cache_data
def get_data(selected_category, selected_level, selected_state):
    hackathon_conn    = st.experimental_connection('snowpark')
    df_DataCommonsAgg =  hackathon_conn.session.table("DATA_COMMONS_AGG_filtered")
    source            = df_DataCommonsAgg.filter(  (col('"VARIABLE_NAME"') == selected_category) 
                                                 & (col('"LEVEL"')         == selected_level)
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
df_LKP_State       = hackathon_conn.session.table("LKP_States")
df_LKP_Levels      = hackathon_conn.session.table("LKP_LEVELS")
df_LKP_Category    = hackathon_conn.session.table("LKP_Categories")
df_LKP_SubCategory = hackathon_conn.session.table("LKP_SubCategories")

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
df_format = get_data(selected_category, selected_level, selected_state + "Pie Visual")
# START FIRST COLLAPSABLE VISUALIZATION GROUPING. # 
with st.expander(selected_category + " in " + selected_level + " " + selected_state):
    pie_chart = alt.Chart(df_format).mark_arc().encode(
        theta=alt.Theta(field="VALUE", type="quantitative"),
        color=alt.Color(field="GEO_NAME", type="nominal"),
    )
    st.altair_chart(pie_chart, use_container_width=False)    

# start second visualization # 
with st.expander(selected_category + " in " + selected_level + " " + selected_state + "Bubble Visual"):
    chart = alt.Chart(df_format, title="Circle Chart").mark_circle().encode(
            y='VALUE',
            x='GEO_NAME',
            size='sum(VALUE):Q'
        ).interactive()
    st.altair_chart(chart, theme="streamlit", use_container_width=False)
# END second VISUALIZATION # 

# start line visualization # 
with st.expander(selected_category + " in " + selected_level + " " + selected_state  + "Line Visual"):
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