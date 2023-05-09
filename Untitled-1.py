
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

with st.expander(selected_category + selected_level_text + selected_state + " Bubble Visual"):
    chart = alt.Chart(df_format, title="Circle Chart").mark_circle().encode(
            y='VALUE',
            x='GEO_NAME',
            size='sum(VALUE):Q',
            color="GEO_NAME", 
        ).interactive()
    st.altair_chart(chart, theme="streamlit", use_container_width=True)
 
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