import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_HEAT_MAP_PATH'] = 's3a://delta-lake-tables/udw/heat_map.csv'
os.environ['HEAT_MAP_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/heat_map'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_heat_map_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        heat_map_src_file_path = os.environ.get("UDW_HEAT_MAP_PATH", "")
        if not heat_map_src_file_path:
            exit_with_error(logger, f"heat_map source file path should be provided!")

        heat_map_delta_table_path = os.environ.get("HEAT_MAP_DELTA_TABLE_PATH", "")
        if not heat_map_delta_table_path:
            exit_with_error(logger, f"heat_map delta table path should be provided!")

        # read heat_map source csv file from s3 bucket
        logger.warn(f"read heat_map source csv file from s3")
        src_df = spark.read.csv(heat_map_src_file_path, header=True)
        heat_map_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("zip", get_column(src_df, "zip", StringType())) \
            .withColumn("caner_type", get_column(src_df, "CanerType", StringType())) \
            .withColumn("biomarker", get_column(src_df, "Biomarker", StringType())) \
            .withColumn("test_result", get_column(src_df, "Test_Result", StringType())) \
            .withColumn("biomarker_id", get_column(src_df, "BIOMARKER_ID", IntegerType())) \
            .select([
                "client_name",
                "health_token_id",
                "zip",
                "caner_type",
                "biomarker",
                "test_result",
                "biomarker_id"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.heat_map (
                client_name STRING,
                health_token_id BINARY,
                zip STRING,
                caner_type STRING,
                biomarker STRING,
                test_result STRING,
                biomarker_id INTEGER
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/heat_map'
        """)

        # write data into delta lake heat_map_df table
        heat_map_df.write.format("delta").mode("append").save(heat_map_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
