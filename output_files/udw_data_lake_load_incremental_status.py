import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_DATA_LAKE_LOAD_INCREMENTAL_STATUS_PATH'] = 's3a://delta-lake-tables/udw/data_lake_load_incremental_status.csv'
os.environ['DATA_LAKE_LOAD_INCREMENTAL_STATUS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/data_lake_load_incremental_status'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_data_lake_load_incremental_status_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        data_lake_load_incremental_status_src_file_path = os.environ.get("UDW_DATA_LAKE_LOAD_INCREMENTAL_STATUS_PATH", "")
        if not data_lake_load_incremental_status_src_file_path:
            exit_with_error(logger, f"data_lake_load_incremental_status source file path should be provided!")

        data_lake_load_incremental_status_delta_table_path = os.environ.get("DATA_LAKE_LOAD_INCREMENTAL_STATUS_DELTA_TABLE_PATH", "")
        if not data_lake_load_incremental_status_delta_table_path:
            exit_with_error(logger, f"data_lake_load_incremental_status delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read data_lake_load_incremental_status source csv file from s3")
        src_df = spark.read.csv(data_lake_load_incremental_status_src_file_path, header=True)
        data_lake_load_incremental_status_df = src_df \
            .withColumn("client_number", get_column(src_df, "ClientNumber", IntegerType())) \
            .withColumn("client_id", get_column(src_df, "Client_ID", StringType())) \
            .withColumn("client_name", get_column(src_df, "Client_Name", StringType())) \
            .withColumn("client_type", get_column(src_df, "Client_Type", StringType())) \
            .withColumn("state", get_column(src_df, "State", StringType())) \
            .withColumn("ocm_y/n", get_column(src_df, "OCM_(Y/N)", StringType())) \
            .withColumn("source_from", get_column(src_df, "SourceFrom", StringType())) \
            .withColumn("frequency", get_column(src_df, "Frequency", StringType())) \
            .withColumn("satus_for_december", get_column(src_df, "Satus_for_December", StringType())) \
            .withColumn("load_status", get_column(src_df, "Load_Status", DateType())) \
            .withColumn("member_count", get_column(src_df, "Member_Count", FloatType())) \
            .withColumn("claims", get_column(src_df, "Claims", StringType())) \
            .withColumn("clinical", get_column(src_df, "Clinical", StringType())) \
            .withColumn("masters", get_column(src_df, "Masters", StringType())) \
            .withColumn("measures", get_column(src_df, "Measures", StringType())) \
            .withColumn("oncology", get_column(src_df, "Oncology", StringType())) \
            .withColumn("comments", get_column(src_df, "Comments", StringType())) \
            .withColumn("source", get_column(src_df, "Source", StringType())) \
            .withColumn("row_id", get_column(src_df, "RowID", IntegerType())) \
            .select([
                "client_number",
                "client_id",
                "client_name",
                "client_type",
                "state",
                "ocm_y/n",
                "source_from",
                "frequency",
                "satus_for_december",
                "load_status",
                "member_count",
                "claims",
                "clinical",
                "masters",
                "measures",
                "oncology",
                "comments",
                "source",
                "row_id",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.data_lake_load_incremental_status (
                client_number INTEGER,
                client_id STRING,
                client_name STRING,
                client_type STRING,
                state STRING,
                ocm_yn STRING,
                source_from STRING,
                frequency STRING,
                satus_for_december STRING,
                load_status DATE,
                member_count FLOAT,
                claims STRING,
                clinical STRING,
                masters STRING,
                measures STRING,
                oncology STRING,
                comments STRING,
                source STRING,
                row_id INTEGER
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/data_lake_load_incremental_status'
        """)

        # write data into delta lake practice table
        data_lake_load_incremental_status_df.write.format("delta").mode("append").save(data_lake_load_incremental_status_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
