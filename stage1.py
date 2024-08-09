from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
import pandas as pd
import json
import sys

def load_schema(schema_file):
    with open(schema_file, 'r') as f:
        schema_json = json.load(f)
        schema = StructType.fromJson(schema_json)
    return schema

def process_data(csv_data, schema_file, output_folder):
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("FilmDataProcessing").getOrCreate()

        # Load schema
        schema = load_schema(schema_file)

        # Load data using Pandas
        df = pd.read_csv(csv_data)

        # Insert 'id' column
        df.insert(0, 'id', df.index + 1)

        # Convert 'release_year' to datetime
        df['release_year'] = pd.to_datetime(df['release_year'], format='%Y', errors='coerce')

        # Extract the integer part from 'durationMins' column
        df['duration'] = df['durationMins'].str.extract(r'(\d+)').astype(int, errors='ignore')

        # Convert to Spark DataFrame with schema
        df_spark = spark.createDataFrame(df, schema=schema)

        # Save the DataFrame as a Parquet file
        df_spark.write.mode("overwrite").parquet(f"{output_folder}/films.parquet")

    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    import sys
    process_data(sys.argv[1], sys.argv[2], sys.argv[3])
