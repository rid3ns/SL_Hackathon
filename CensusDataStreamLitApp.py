#snowpark
from snowflake.snowpark.session   import Session
from snowflake.snowpark.types     import IntegerType
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
import sys


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

# # Initialize snowpark connection. 
# hackathon_conn = st.experimental_connection('snowpark')
# print(hackathon_conn.session.sql('select current_warehouse(), current_database(), current_schema()').collect())

# # Start Side Panel # - This code creates the side panel which filters the overall site 
# level_list        = []
# category_list     = []
# sub_category_list = []

# # tables to use throughout dashboard
# df_DataCommonsAgg  = hackathon_conn.session.table("DATA_COMMONS_AGG")
# df_LKP_Levels      = hackathon_conn.session.table("LKP_LEVELS")
# df_LKP_Category    = hackathon_conn.session.table("LKP_Categories")
# df_LKP_SubCategory = hackathon_conn.session.table("LKP_SubCategories")

# # Create Level Dataframe  from df_LKP_Levels
# # allow user to choose location filter type for data
# level_list     = df_LKP_Levels.select(col("LEVEL")).distinct().to_pandas()   
# with st.sidebar:
#     with st.spinner("Loading.. "):
#         selected_level = st.sidebar.selectbox('Select the level you\'d like to filter to.', level_list)

# # Create Category Dataframe from df_LKP_Category
# # allow user to choose category
# category_list     = df_LKP_Category.select(col("CATEGORY")).distinct().to_pandas()
# with st.sidebar:
#     with st.spinner("Loading.. "):
#         selected_category = st.sidebar.selectbox('Select the category you\'d like to see.', category_list)

# # Create subcategory Dataframe
# # allow user to choose subcategory
# sub_category_list   = df_LKP_SubCategory.select(col("SUB_CATEGORY")).distinct().to_pandas()
# with st.sidebar:
#     with st.spinner("Loading.. "):
#         selected_sub_category = st.sidebar.selectbox('Select the sub-category you\'d like to see.', sub_category_list)

#END Side Panel #
# if selected_sub_category is not None:
#     variable_value = selected_category + ': ' + selected_sub_category

# df_barchart_format = df_DataCommonsAgg.filter((col('"VARIABLE_NAME"') == selected_category) & (col('"LEVEL"') == selected_level)).select(col("VALUE").as_("VALUE"), col("GEO_NAME")).group_by("GEO_NAME").sum("VALUE")
# df_barchart        = df_barchart_format.select(col("SUM(VALUE)").as_("VALUE"), col("GEO_NAME")).to_pandas()

# # START FIRST COLLAPSABLE VISUALIZATION GROUPING. # 
#     # Using expanders for each section for ease of access and smaller initial page size
# with st.expander(f'Testing Dynamic SQL Based On Categories'):
#     bar_chart = alt.Chart(df_barchart).mark_bar().encode(
#         y='VALUE',
#         x='GEO_NAME',
#     )
#     st.altair_chart(bar_chart, use_container_width=True)
    
# # END FIRST VISUALIZATION # 
