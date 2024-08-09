from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def process_genres_parquet(input_path, output_folder):
    # Initialize Spark session
    spark = SparkSession.builder.appName("GenreProcessing").getOrCreate()

    # Read the parquet file created in stage 1
    df_all_films = spark.read.parquet(f"{input_path}/films.parquet")

    # Split genres into separate rows and remove underscores
    df_genres = df_all_films.withColumn("genre", explode(split(col("genres"), ", ")))

    # Write each genre to a separate parquet file
    for genre in df_genres.select("genre").distinct().collect():
        genre_name = genre["genre"].replace(" ", "")
        df_genres.filter(col("genre") == genre["genre"]).write.mode("overwrite").parquet(f"{output_folder}/{genre_name}.parquet")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    import sys
    process_genres_parquet(sys.argv[1], sys.argv[2])
