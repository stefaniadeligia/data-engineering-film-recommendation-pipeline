import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestQueryFilms").getOrCreate()

@pytest.fixture(scope="module")
def test_data(spark):
    # Read the parquet file
    df = spark.read.parquet("output/films.parquet")
    return df

@pytest.fixture(scope="module")
def parquet_path(test_data, tmp_path_factory):
    # Save the DataFrame to a temporary Parquet file for testing
    path = tmp_path_factory.mktemp("data") / "films.parquet"
    test_data.write.parquet(str(path))
    return str(path)

def test_query_by_director(spark, parquet_path):
    filters = {"director": "Quentin Tarantino"}
    result_df = query_films(parquet_path, filters)
    assert result_df.filter(col("director") == "Quentin Tarantino").count() == 7 # There are 7 movies directed by Quentin Tarantino

def test_query_by_release_year_range(spark, parquet_path):
    filters = {"release_year": ("2000-01-01", "2001-01-01")}
    result_df = query_films(filters)  # Assuming query_films is updated to take parquet_path
    assert result_df.count() == 51  # Update the count to match your test data

def test_query_by_country(spark, parquet_path):
    filters = {"country": "Italy"}
    result_df = query_films(parquet_path, filters)
    assert result_df.count() == 61  # There are 61 Italian movies
