import os
import pyspark
import dask.dataframe as dd
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define Paths for Input and Output
INPUT_PATH_TRACT = "/Users/antropravin/Documents/GitHub Project/Big Data Processing Without Cloud/Dataset/acs2015_census_tract_data.csv"
INPUT_PATH_COUNTY = "/Users/antropravin/Documents/GitHub Project/Big Data Processing Without Cloud/Dataset/acs2015_county_data.csv"
OUTPUT_PATH = "/Users/antropravin/Documents/GitHub Project/Big Data Processing Without Cloud"
DUCKDB_FILE = os.path.join(OUTPUT_PATH, "census_data.duckdb")

# Make sure the output directory exists
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Step 1: Initialize Spark Session
print("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("US Census Data Processing") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Step 2: Load CSV Datasets Using PySpark
print("Loading datasets with PySpark...")
df_tract = spark.read.csv(INPUT_PATH_TRACT, header=True, inferSchema=True)
df_county = spark.read.csv(INPUT_PATH_COUNTY, header=True, inferSchema=True)

# Display the schema of the datasets
print("Data Schema:")
df_tract.printSchema()
df_county.printSchema()

# Step 3: Load Datasets Using Dask for Parallel Processing
print("Loading datasets using Dask...")
dask_df_tract = dd.read_csv(INPUT_PATH_TRACT)
dask_df_county = dd.read_csv(INPUT_PATH_COUNTY)

# Step 4: Clean and Prepare the Data
print("Cleaning and transforming datasets...")

# Clean the PySpark data (remove nulls and cast TotalPop to integer)
df_tract = df_tract.dropna().withColumn("TotalPop", col("TotalPop").cast("int"))
df_county = df_county.dropna().withColumn("TotalPop", col("TotalPop").cast("int"))

# Clean the Dask data (remove nulls and cast TotalPop to integer)
dask_df_tract = dask_df_tract.dropna()
dask_df_county = dask_df_county.dropna()
dask_df_tract["TotalPop"] = dask_df_tract["TotalPop"].astype("int32")
dask_df_county["TotalPop"] = dask_df_county["TotalPop"].astype("int32")

# Step 5: Save Data to Parquet for Efficient Storage
#print("Saving cleaned data as Parquet files...")

# Saving with PySpark
#df_tract.write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "tract_data.parquet"))
#df_county.write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "county_data.parquet"))

# Saving with Dask
#dask_df_tract.to_parquet(os.path.join(OUTPUT_PATH, "tract_data_dask.parquet"), engine="pyarrow", write_index=False)
#dask_df_county.to_parquet(os.path.join(OUTPUT_PATH, "county_data_dask.parquet"), engine="pyarrow", write_index=False)

#print("Data storage optimization complete.")

# Step 6: Create a DuckDB Database
# If the database already exists, delete it to start fresh
if os.path.exists(DUCKDB_FILE):
    os.remove(DUCKDB_FILE)
    print("Old DuckDB database deleted.")

# Now, create a fresh DuckDB database
print("Creating a new DuckDB database...")
con = duckdb.connect(DUCKDB_FILE)

# Step 7: Load Parquet Files into DuckDB for Fast Querying
try:
    duckdb_tract_path = os.path.join(OUTPUT_PATH, "tract_data.parquet")
    duckdb_county_path = os.path.join(OUTPUT_PATH, "county_data.parquet")

    # Create DuckDB tables from the Parquet files
    con.execute(f"CREATE TABLE tract AS SELECT * FROM read_parquet('{duckdb_tract_path}/*.parquet')")
    con.execute(f"CREATE TABLE county AS SELECT * FROM read_parquet('{duckdb_county_path}/*.parquet')")

    print("DuckDB tables created!")

    # Step 8: Run Sample Queries on DuckDB to Explore Data
    print("Sample Query: Total Population by State")
    result = con.execute("SELECT State, SUM(TotalPop) AS TotalPopulation FROM county GROUP BY State ORDER BY TotalPopulation DESC").fetchdf()
    print(result)

    print("Basic Statistics: County Data")
    county_summary = con.execute("""
        SELECT 
            COUNT(*) AS TotalRecords,
            AVG(TotalPop) AS AvgPopulation,
            MIN(TotalPop) AS MinPopulation,
            MAX(TotalPop) AS MaxPopulation
        FROM county
    """).fetchdf()
    print(county_summary)

    print("Top 5 Counties by Population")
    top_counties = con.execute("""
        SELECT County, State, TotalPop
        FROM county
        ORDER BY TotalPop DESC
        LIMIT 5
    """).fetchdf()
    print(top_counties)

    print("Joining Tract and County Data")
    join_result = con.execute("""
        SELECT c.State, c.County, t.TotalPop
        FROM tract t
        JOIN county c
        ON t.State = c.State
        ORDER BY t.TotalPop DESC
        LIMIT 10
    """).fetchdf()
    print(join_result)

    print("Population Density Analysis")
    pop_density = con.execute("""
        SELECT State, SUM(TotalPop) AS TotalPopulation, COUNT(*) AS NumberOfTracts,
               SUM(TotalPop) / COUNT(*) AS AvgPopulationPerTract
        FROM tract
        GROUP BY State
        ORDER BY AvgPopulationPerTract DESC
        LIMIT 10
    """).fetchdf()
    print(pop_density)

    # Step 9: Save Query Results to CSV Files
    county_summary.to_csv(os.path.join(OUTPUT_PATH, "county_summary.csv"), index=False)
    top_counties.to_csv(os.path.join(OUTPUT_PATH, "top_counties.csv"), index=False)
    join_result.to_csv(os.path.join(OUTPUT_PATH, "join_result.csv"), index=False)
    pop_density.to_csv(os.path.join(OUTPUT_PATH, "pop_density.csv"), index=False)

    print("Results exported to CSV.")

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    # Close the DuckDB connection
    con.close()
    print("DuckDB database setup and queries complete!")

# Step 10: Stop the Spark Session 
spark.stop()
print("Spark session stopped.")