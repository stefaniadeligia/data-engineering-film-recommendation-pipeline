# Import Statements
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, array_intersect, array_contains, lit
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
import os

# Initialize Spark Session
spark = SparkSession.builder \
        .appName("FilmSimilarity") \
        .getOrCreate()

# Function to detect similarity based on film ID and a similarity threshold
def detect_similarity(film_id, threshold, output_path):
    df_all_films = spark.read.parquet(f"{output_path}/films.parquet")
    # Get the film details for the given film_id
    film_details = df_all_films.filter(col("id") == film_id).collect()[0]

    # Extract the first three words of the title
    target_title_first_three_words = ' '.join(film_details["title"].split()[:3])

    # Calculate similarity based on the first three words of the title, director, and genres
    similar_films = df_all_films.withColumn(
        "similarity",
        (
            (col("title").like(f"%{target_title_first_three_words}%")).cast("int") * 0.5 +  # Partial title match
            (col("director") == film_details["director"]).cast("int") * 0.25 +
            (array_contains(split(col("genres"), ", "), film_details["genres"])).cast("int") * 0.25
        )
    )

    # Filter films with similarity above the threshold
    result = similar_films.filter(col("similarity") > threshold)

    return result

# Example usage of the detect_similarity function
if __name__ == "__main__":
    # Ask for input from the user
    film_id = input("Enter film ID (default 0): ") or "0"
    threshold = input("Enter similarity threshold (default 0.5): ") or "0.5"

    # Convert inputs to the appropriate data types
    film_id = int(film_id)
    threshold = float(threshold)
    output_path = "output"

    # Call the function and get the result
    result_df = detect_similarity(film_id, threshold, output_path)
    if result_df:
        result_df.show()  # Display the result DataFrame