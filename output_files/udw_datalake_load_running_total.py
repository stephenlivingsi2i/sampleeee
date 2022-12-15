import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_DATA_LAKE_LOAD_RUNNING_TOTAL_PATH'] = 's3a://delta-lake-tables/udw/data_lake_load_running_total.csv'
os.environ['DATA_LAKE_LOAD_RUNNING_TOTAL_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/data_lake_load_running_total'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_data_lake_load_running_total_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        data_lake_load_running_total_src_file_path = os.environ.get("UDW_DATA_LAKE_LOAD_RUNNING_TOTAL_PATH", "")
        if not data_lake_load_running_total_src_file_path:
            exit_with_error(logger, f"datalake_load_running_total source file path should be provided!")

        datalake_load_running_total_delta_table_path = os.environ.get("DATA_LAKE_LOAD_RUNNING_TOTAL_DELTA_TABLE_PATH", "")
        if not datalake_load_running_total_delta_table_path:
            exit_with_error(logger, f"datalake_load_running_total delta table path should be provided!")

        # read datalake_load_running_total source csv file from s3 bucket
        logger.warn(f"read datalake_load_running_total source csv file from s3")
        src_df = spark.read.csv(data_lake_load_running_total_src_file_path, header=True)
        datalake_load_running_total_df = src_df \
            .withColumn("client_id", get_column(src_df, "clientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "clientName", StringType())) \
            .withColumn("yr", get_column(src_df, "YR", IntegerType())) \
            .withColumn("mth", get_column(src_df, "MTH", IntegerType())) \
            .withColumn("running_total", get_column(src_df, "RunningTotal", IntegerType())) \
            .select([
                "client_id",
                "client_name",
                "yr",
                "mth",
                "running_total"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.practice (
                client_id INTEGER,
                client_name STRING,
                yr INTEGER,
                mth INTEGER,
                running_total INTEGER
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/datalake_load_running_total'
        """)

        # write data into delta lake datalake_load_running_total table
        datalake_load_running_total_df.write.format("delta").mode("append").save(datalake_load_running_total_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
