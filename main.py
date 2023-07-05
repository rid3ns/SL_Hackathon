import streamlit as st
import pandas as pd
import numpy as np
import matplotlib as mpl
import snowflake.connector
from langchain.agents import create_pandas_dataframe_agent, load_tools, tools, initialize_agent
from langchain.chat_models import ChatOpenAI
from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.callbacks import StreamlitCallbackHandler

#from dotenv import load_dotenv
from tenacity import (retry, stop_after_attempt, wait_random_exponential)

def main():
    #load_dotenv('ArmetaHackathon_PV3.10\.env')
    llm = ChatOpenAI(openai_api_key='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')

    conn = snowflake.connector.connect(
    user='CSUMMERS',
    password='Armeta@1812',
    account='zs31584.east-us-2.azure',
    role='SF_HACKATHON_ROLE',
    warehouse='SF_HACKATHON_WH',
    database='SF_HACKATHON',
    schema='Test Team'
    )

    cur = conn.cursor()
      
    #@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
    #def getresponse(agent, query):
    #    return agent.run(query)
    
    st.set_page_config(page_title="Tabular Profiling Tool")
    st.header("Tabular Profiling Tool")

    #import snowflake tables
    showtables = 'SHOW TABLES in SF_HACKATHON."Test Team"'
    resulttables = 'SELECT "name" as Dataset FROM table(RESULT_SCAN(LAST_QUERY_ID()))'

    #Executes query that returns table names in the Test Team schema
    cur.execute(showtables)
    cur.execute(resulttables)

    tablesdf = cur.fetch_pandas_all()
    st.write(tablesdf)

    tableoption = st.selectbox('Select the dataset you would like to use: ', tablesdf.values)

    actionoption = st.selectbox('What action would you like to perform?', ('Describe Relationship Between Columns', 'Custom Prompt'))

    importquery = 'SELECT * FROM SF_HACKATHON."Test Team".'+ tableoption[0] + ' LIMIT 100'
    cur.execute(importquery)
    df = cur.fetch_pandas_all()
    st.write('Here is a sample output of your selected dataset:')
    st.table(df.head(5))


     
    def colrel(df):
        option1 = st.selectbox('Select the first column: ', df.columns)
        option2 = st.selectbox('Select the second column: ', df.columns)
        
        if st.button('Select 2 columns'):
            st.write('Answer:')
            mystring(option1, option2, df)
        else: st.write('')

    def CustomPrompt(df):
        query = st.text_input("Chat with your table:")
        if st.button('Click to Execute'):
            st.write('Answer:')
            myagent(query, df)

    def mystring(col1, col2, df):
        thestring = "What is the relationship between columns " + col1 + " and " + col2
        #return thestring
        myagent(thestring, df)
    
    def myagent(query, df):
        agent = create_pandas_dataframe_agent(ChatOpenAI(temperature=0, model="gpt-3.5-turbo-16k"), df, verbose=True)
        #agent.run(query)
        st_callback = StreamlitCallbackHandler(st.container())
        response = agent.run(query, callbacks=[st_callback])
        st.info(response) 

    if actionoption == 'Describe Relationship Between Columns':
        colrel(df)
    else:
        CustomPrompt(df)

    df = cur.fetch_pandas_all()

    #st.table(tablesdf.head(5))

if __name__ == "__main__":
    main()