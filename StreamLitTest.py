import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import sys
import pgeocode
from pydeck.types import String

# Setting up geocoding for United States
nomi = pgeocode.Nominatim('US')

# Page Config
st.set_page_config(page_title="Testing Streamlit", layout="wide")

# Page Header/Subheader
st.title("Hello World!")
st.subheader("We data profilin', we looking at that data")

with st.sidebar.expander("About the App"):
     st.write("""
        This data profiling App was built by My Data Talk using Streamlit and pandas_profiling package. You can use the app to quickly generate a comprehensive data profiling and EDA report without the need to write any python code. \n\nThe app has the minimum mode (recommended) and the complete code. The complete code includes more sophisticated analysis such as correlation analysis or interactions between variables which may requires expensive computations. )
     """)

# Initialize snowpark connection. 
hackathon_conn = st.experimental_connection('snowpark')

# Using expanders for each section for ease of access and smaller initial page size
with st.expander(f'Combining Datasets'):

    df = hackathon_conn.query(f"SELECT DISTINCT LEVEL FROM GEO_INDEX ORDER BY LEVEL;")
    st.dataframe(df)

    df = hackathon_conn.query(f"SELECT * FROM GEO_INDEX WHERE GEO_NAME LIKE '%Texas%';")
    st.dataframe(df)

    df = hackathon_conn.query(f'''  SELECT COUNT(*) FROM GEO_INDEX GI
                                    JOIN GEO_OVERLAPS GO ON GI.ID = GO.GEO_ID
                                    JOIN TIMESERIES TS ON GI.ID = TS.GEO_ID
                                    WHERE GEO_NAME LIKE '%Texas%';''')
    st.dataframe(df)

    df = hackathon_conn.query(f"SELECT DISTINCT GEO_NAME, LEVEL FROM GEO_INDEX WHERE GEO_NAME LIKE '%Texas%' AND LEVEL <> 'CensusTract';")
    st.dataframe(df)

    df = hackathon_conn.query(f"SELECT DISTINCT GEO_NAME, LEVEL FROM GEO_INDEX WHERE GEO_NAME LIKE '%Texas%' AND LEVEL = 'City';")
    st.dataframe(df)

    df = hackathon_conn.query(f'''  SELECT DISTINCT TS.CATEGORY
                                    FROM GEO_INDEX          GI
                                        JOIN GEO_OVERLAPS   GO ON GI.ID         = GO.GEO_ID
                                        JOIN TIMESERIES     TS ON GI.ID         = TS.GEO_ID
                                    WHERE GI.GEO_NAME LIKE '%Texas%' 
                                    ORDER BY TS.CATEGORY ''')
    st.dataframe(df)

    df = hackathon_conn.query(f'''  

SELECT TOP 1000 GI.ID, GI.GEO_NAME, GI.LEVEL, GO.OVERLAPS_WITH, TS.VARIABLE, TS.VARIABLE_NAME, TS.DATE, TS.VALUE, TS.UNIT, TS.CATEGORY, TS.MEASUREMENT_METHOD 
FROM GEO_INDEX          GI
    JOIN GEO_OVERLAPS   GO ON GI.ID         = GO.GEO_ID
    JOIN TIMESERIES     TS ON GI.ID         = TS.GEO_ID
WHERE GI.GEO_NAME LIKE '%Texas%'
    AND LEVEL = 'City'
--GROUP BY GI.ID, GI.GEO_NAME, GI.LEVEL, TS.VARIABLE, TS.VARIABLE_NAME, TS.DATE, TS.VALUE, TS.UNIT, TS.CATEGORY, TS.MEASUREMENT_METHOD
ORDER BY GI.ID, TS.VARIABLE, TS.DATE;

''')

    st.dataframe(df)

    df = hackathon_conn.query(f'''  

SELECT GI.ID, GI.GEO_NAME, GI.LEVEL, listagg(REPLACE(GO.OVERLAPS_WITH, 'zip/', ''), ',') as ZIP_CODES, TS.VARIABLE, TS.VARIABLE_NAME, TS.DATE, TS.VALUE, TS.UNIT, TS.CATEGORY, TS.MEASUREMENT_METHOD 
FROM GEO_INDEX          GI
    JOIN GEO_OVERLAPS   GO ON GI.ID         = GO.GEO_ID
    JOIN TIMESERIES     TS ON GI.ID         = TS.GEO_ID
WHERE GI.GEO_NAME LIKE '%Texas%'
    AND LEVEL       = 'City'
    AND VARIABLE LIKE '%Criminal%'
GROUP BY GI.ID, GI.GEO_NAME, GI.LEVEL, TS.VARIABLE, TS.VARIABLE_NAME, TS.DATE, TS.VALUE, TS.UNIT, TS.CATEGORY, TS.MEASUREMENT_METHOD
ORDER BY GI.ID, TS.VARIABLE, TS.DATE;

''')

    st.dataframe(df)
    
    map_df = None

    for row in df.itertuples():

        zip_code_list = row.ZIP_CODES.split(",")
        location_df = nomi.query_postal_code(zip_code_list)

        elevation = row.VALUE

        #st.write(zip_code_list)
        #st.dataframe(location_df)

        temp_map_df = pd.DataFrame().assign(lat=location_df['latitude'], lon=location_df['longitude'], elevation=elevation)
        map_df = pd.concat([map_df, temp_map_df])

    st.dataframe(map_df)

    st.pydeck_chart(pdk.Deck(
        map_style='dark',
        #height=st.screen_height * 0.5,
        initial_view_state=pdk.ViewState(
            latitude=30.9433703,
            longitude=-99.7004626,
            zoom=5.7,
            pitch=25,
        ),
        layers=[
            #pdk.Layer(
            #    'HexagonLayer',
            #    data=map_df,
            #    get_position='[lon, lat]',
            #    radius=1000,
            #    elevation_scale=1,
            #    elevation_range=[0, 100],
            #    pickable=True,
            #    extruded=True,
            #    get_elevation="elevation",
            #    coverage=1,
            #),
            pdk.Layer(
                "HeatmapLayer",
                data=map_df,
                opacity=0.9,
                get_position=["lon", "lat"],
                get_elevation="elevation",
            ),
        ],
    ))


# Adding all DATA_COMMONS tables to list so we can loop through list and query each dataset
#data_profile_table_list = ['GEO_INDEX', 'GEO_OVERLAPS', 'MEASURES', 'PUBLIC_HOLIDAYS', 'TIMESERIES', 'VARIABLE_SUMMARY']
#
#for table in data_profile_table_list:
#
#    table_row_count = hackathon_conn.query(f'SELECT COUNT(*) as CNT FROM {table};').iloc[0].CNT
#
#    df = hackathon_conn.query(f'SELECT TOP 1000 * FROM {table};')
#
#    with st.expander(f'Profile of {table}'):
#        st.write(f'Row Count: {table_row_count}')
#        st.write(f'Top 1000 results:')
#        st.dataframe(df)
#        st.write(f'Unique values in data frame:')
#
#        try:
#            st.write(pd.Series({c: df[c].unique() for c in df}))
#        except:
#            st.write(sys.exc_info()[0])



# Testing connection with query
#df = hackathon_conn.query('SELECT TOP 1 * FROM TEST_TABLE;', ttl=600)

#for row in df.itertuples():
#    st.write(f"ID: {row.ID} | GEO_NAME: {row.GEO_NAME}")