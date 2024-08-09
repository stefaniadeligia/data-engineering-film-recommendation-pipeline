from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def query_films(filters):
    # Initialize Spark session
    spark = SparkSession.builder.appName("FilmQuery").getOrCreate()
    
    # Read the parquet file
    df = spark.read.parquet("output/films.parquet")
    
    # Apply filters
    for column, criteria in filters.items():
        if isinstance(criteria, tuple) and len(criteria) == 2:
            # Range filter
            df = df.filter((col(column) >= criteria[0]) & (col(column) <= criteria[1]))
        elif isinstance(criteria, list):
            # Set filter
            df = df.filter(col(column).isin(criteria))
        else:
            # Equality filter
            df = df.filter(col(column) == criteria)
    
    return df
