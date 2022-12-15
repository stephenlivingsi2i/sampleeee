import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_HEAT_MAP_NO_BIOMARKER_PATH'] = 's3a://delta-lake-tables/udw/heat_map_no_biomarker.csv'
os.environ['HEAT_MAP_NO_BIOMARKER_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/heat_map_no_biomarker'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_heat_map_no_biomarker_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        heat_map_no_biomarker_src_file_path = os.environ.get("UDW_HEAT_MAP_NO_BIOMARKER_PATH", "")
        if not heat_map_no_biomarker_src_file_path:
            exit_with_error(logger, f"heat_map_no_biomarker source file path should be provided!")

        heat_map_no_biomarker_delta_table_path = os.environ.get("HEAT_MAP_NO_BIOMARKER_DELTA_TABLE_PATH", "")
        if not heat_map_no_biomarker_delta_table_path:
            exit_with_error(logger, f"heat_map_no_biomarker delta table path should be provided!")

        # read heat_map_no_biomarker source csv file from s3 bucket
        logger.warn(f"read heat_map_no_biomarker source csv file from s3")
        src_df = spark.read.csv(heat_map_no_biomarker_src_file_path, header=True)
        heat_map_no_biomarker_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("zip", get_column(src_df, "zip", StringType())) \
            .withColumn("city", get_column(src_df, "city", StringType())) \
            .withColumn("state", get_column(src_df, "STATE", StringType())) \
            .withColumn("cancer_type", get_column(src_df, "CancerType", StringType())) \
            .withColumn("client_type", get_column(src_df, "ClientType", StringType())) \
            .select([
                "client_name",
                "health_token_id",
                "zip",
                "city",
                "state",
                "cancer_type",
                "client_type"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.heat_map_no_biomarker (
                client_name STRING,
                health_token_id BINARY,
                zip STRING,
                city STRING,
                state STRING,
                cancer_type STRING,
                client_type STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/heat_map_no_biomarker'
        """)

        # write data into delta lake heat_map_no_biomarker_df table
        heat_map_no_biomarker_df.write.format("delta").mode("append").save(heat_map_no_biomarker_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
