import streamlit as st
import pandas as pd
import duckdb
import os

# Define paths
OUTPUT_PATH = "/Users/antropravin/Desktop/Bezohminds/Task/Big Data Processing Without Cloud"
DUCKDB_FILE = os.path.join(OUTPUT_PATH, "census_data.duckdb")

# Connect to DuckDB
con = duckdb.connect(database=DUCKDB_FILE, read_only=True)

# Streamlit UI
st.title("US Census Data Dashboard")
st.sidebar.header("Navigation")

# Display summary statistics
if st.sidebar.button("View Summary Statistics"):
    query = """
        SELECT COUNT(*) AS TotalRecords, 
               AVG(TotalPop) AS AvgPopulation, 
               MIN(TotalPop) AS MinPopulation, 
               MAX(TotalPop) AS MaxPopulation 
        FROM county
    """
    result = con.execute(query).fetchdf()
    st.subheader("Summary Statistics")
    st.dataframe(result)

# Display top counties by population
limit = st.sidebar.slider("Top N", 1, 10, 6)  

# Query for Top N Counties
if st.sidebar.button("Show Top Counties"):
    query = f"""
        SELECT County, State, TotalPop 
        FROM county 
        ORDER BY TotalPop DESC 
        LIMIT {limit}
    """
    result = con.execute(query).fetchdf()
    st.subheader(f"Top {limit} Counties by Population")
    st.dataframe(result)

# Query for Top Population Density Countries (Now using the common limit)
if st.sidebar.button("Show Population Density"):
    query = f"""
        SELECT State, SUM(TotalPop) AS TotalPopulation, COUNT(*) AS NumberOfTracts, 
               SUM(TotalPop) / COUNT(*) AS AvgPopulationPerTract 
        FROM tract 
        GROUP BY State 
        ORDER BY AvgPopulationPerTract DESC 
        LIMIT {limit}
    """
    result = con.execute(query).fetchdf()
    st.subheader(f"Top {limit} States by Population Density")
    st.dataframe(result)

# Close connection
con.close()

st.sidebar.write("Developed using Streamlit & DuckDB")