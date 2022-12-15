import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_ECOG_SCORE_PATH'] = 's3a://delta-lake-tables/udw/ecog_score.csv'
os.environ['ECOG_SCORE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/ecog_score'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_ecog_score_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ecog_score_src_file_path = os.environ.get("UDW_ECOG_SCORE_PATH", "")
        if not ecog_score_src_file_path:
            exit_with_error(logger, f"ecog_score source file path should be provided!")

        ecog_score_delta_table_path = os.environ.get("ECOG_SCORE_DELTA_TABLE_PATH", "")
        if not ecog_score_delta_table_path:
            exit_with_error(logger, f"ecog_score delta table path should be provided!")

        # read dim_dx_group source csv file from s3 bucket
        logger.warn(f"read ecog_score source csv file from s3")
        src_df = spark.read.csv(ecog_score_src_file_path, header=True)
        ecog_score_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("ecog_score", get_column(src_df, "ECOG_SCORE", StringType())) \
            .withColumn("ecog_documented_date", get_column(src_df, "ECOG_Documented_Date", DateType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .select([
                "client_name",
                "client_id",
                "health_token_id",
                "ecog_score",
                "ecog_documented_date",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.ecog_score (
                client_name STRING,
                client_id INTEGER,
                health_token_id BINARY,
                ecog_score STRING,
                ecog_documented_date DATE,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/ecog_score'
        """)

        # write data into delta lake dim_dx_group table
        ecog_score_df.write.format("delta").mode("append").save(ecog_score_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
