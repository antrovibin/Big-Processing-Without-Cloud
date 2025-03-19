# Big-Processing-Without-Cloud

This project demonstrates the processing of large U.S. Census datasets without relying on cloud services. The task involves handling large datasets efficiently, performing data processing, and running analyses using tools like PySpark, Dask, and DuckDB, all configured based on system specifications.

### Overview

The objective of this project is to process large datasets (ACS Census data for tracts and counties) locally, optimizing for memory and processing efficiency. The solution is built with flexibility to adapt to the system’s resources, ensuring smooth execution even with large volumes of data.

### Technologies Used

**PySpark:** Distributed data processing framework for large datasets.
**Dask:** Parallel computing library used for handling larger-than-memory datasets.
**DuckDB:** In-memory SQL analytics engine to query large datasets stored in Parquet format.
**Pandas:** Used for managing smaller datasets and intermediate steps.

### Dataset

The datasets used in this project are U.S. Census data files for the year 2015, including:

**Tract data:** Detailed census data at the census tract level.
**County data:** Census data aggregated at the county level.
These datasets are in CSV format and have been processed, cleaned, and stored as Parquet files for efficient storage and analysis.

### Approaches Considered

**1. Data Chunking**
Splitting the large dataset into smaller chunks for batch processing.
While this method helps in handling large datasets, it can reduce processing accuracy and efficiency due to the overhead of chunking and reassembling data.
**2. Disk-Based Storage**
Storing datasets on disk (in formats like CSV or Parquet) to handle larger data than in-memory processing allows.
However, this method still has limitations when handling very large datasets, and performance can degrade as the dataset size increases.

### My Approach

To overcome the limitations mentioned above, I adopted a more system-adaptive solution:

**Configuration-Based Processing:** The solution dynamically adjusts based on available system resources (memory, CPU), ensuring optimal performance without overwhelming the system.
**Spark for Distributed Processing:** PySpark was used for distributed data processing with memory optimization and parallelism.
**Dask for Larger-than-Memory Handling:** Dask was used to handle data that couldn't fit into memory, dividing the dataset into partitions for parallel computation.
**DuckDB for Fast Querying:** DuckDB was used to load Parquet files into a fast in-memory SQL engine, enabling complex queries and analysis on the data.
Steps Taken

**1. Data Loading**
Loaded the census tract and county data using both PySpark and Dask.
Data was cleaned (null handling, type casting) and transformed.
**2. Data Storage**
Cleaned data was saved in Parquet format to improve storage efficiency and facilitate fast querying.
Parquet files were then loaded into DuckDB for SQL-based analysis.
**3. Data Analysis**
Ran various SQL queries on DuckDB to analyze the total population by state, basic statistics on county population, and more.
Joined tract and county data for deeper insights, including population density analysis.
**4. Optimization**
The system was configured with memory limits and optimized for parallel processing to handle large datasets effectively without relying on cloud services.
Project Structure

```bash
/big-data-processing-without-cloud
|-- /Dataset                    # Raw dataset files (CSV format)
|-- /Output                     # Processed data files (Parquet format)
|-- /Code                       # Python scripts for processing and analysis
|-- README.md                   # Project documentation
|-- requirements.txt            # Required Python libraries
```
### Installation

Clone the repository:
```bash
git clone https://github.com/antrovibin/big-data-processing-without-cloud.git
cd big-data-processing-without-cloud
```

Ensure that you have the necessary data files in the /Dataset directory.

### Usage

To run the project, simply execute the main script:
```bash
python process_census_data.py
```
This will:
Load the data.
Clean and preprocess the datasets.
Save the processed data as Parquet files.
Load the data into DuckDB and run predefined queries.
