import pyspark
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.driver.extraClassPath", "/Users/sriha/jdbc/postgresql-42.7.5-all.jar") \
    .getOrCreate()

# Read table from DB using Spark JDBC
def extract_movies_to_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable", "movies") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return movies_df  #  Added return statement

def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable", "users") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return users_df  #  Added return statement

def transform_avg_ratings(movies_df, users_df):
    # Calculate average ratings per movie
    avg_rating = users_df.groupBy("movie_id").mean("rating")
    
    # Rename column from "avg(rating)" to "avg_rating"
    avg_rating = avg_rating.withColumnRenamed("avg(rating)", "avg_rating")
    
    # Join movies table with average ratings on movie ID
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id, "left")
    df = df.drop("movie_id")  # Remove duplicate column
    return df  #  Added return statement

# Load transformed dataframe to the database
def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/etl_pipeline"
    
    #  Fixed dictionary syntax error (added missing commas)
    properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(url=url,
                  table="ratings",  #  Replaced empty table name
                  mode=mode,
                  properties=properties)

if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    ratings_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(ratings_df)  #  Pass transformed DataFrame to load function

    # Print DataFrames for verification
    movies_df.show()
    users_df.show()
    ratings_df.show()  #  Print the transformed DataFrame instead of incorrect join

