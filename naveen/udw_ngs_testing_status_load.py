import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_NGS_TESTING_STATUS_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/NGS_TESTING_STATUS.csv"
# export NGS_TESTING_STATUS_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/NGS_TESTING_STATUS"


def main():
    app_name = "udw_ngs_testing_status_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ngs_testing_status_src_file_path = os.environ.get("UDW_NGS_TESTING_STATUS_FILE_PATH", "")
        if not ngs_testing_status_src_file_path:
            exit_with_error(logger, f"ngs_testing_status source file path should be provided!")

        ngs_testing_status_delta_table_path = os.environ.get("NGS_TESTING_STATUS_DELTA_TABLE_PATH", "")
        if not ngs_testing_status_delta_table_path:
            exit_with_error(logger, f"ngs_testing_status delta table path should be provided!")

        # read ngs_testing_status source csv file from s3 bucket
        logger.warn(f"read ngs_testing_status source csv file from s3")
        src_df = spark.read.csv(ngs_testing_status_src_file_path, header=True)
        
        ngs_testing_status_df = src_df \
            .withColumn('status_id ', get_column(src_df,'Status_id', IntegerType()))\
            .withColumn('health_record_key ', get_column(src_df,'HealthRecordKey', IntegerType()))\
            .withColumn('order_id ', get_column(src_df,'order_id'))\
            .withColumn('order_type ', get_column(src_df,'order_type'))\
            .withColumn('report_date ', get_column(src_df,'report_date', DateType()))\
            .withColumn('specimen_collection_date ', get_column(src_df,'specimen_collection_date', DateType()))\
            .withColumn('specimen_received_date ', get_column(src_df,'specimen_received_date', DateType()))\
            .withColumn('test ', get_column(src_df,'test'))\
            .withColumn('decision ', get_column(src_df,'decision'))\
            .withColumn('failure_reasons ', get_column(src_df,'failure_reasons'))\
            .withColumn('failure_reason ', get_column(src_df,'failure_reason'))\
            .withColumn('signed_by ', get_column(src_df,'signed_by'))\
            .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
            .withColumn('client_name ', get_column(src_df,'ClientName'))\
            .withColumn('health_token_id', get_column(src_df,'HealthTokenID', BinaryType())) \
            .select([
                'status_id ',
                'health_record_key ',
                'order_id ',
                'order_type ',
                'report_date ',
                'specimen_collection_date ',
                'specimen_received_date ',
                'test ',
                'decision ',
                'failure_reasons ',
                'failure_reason ',
                'signed_by ',
                'client_id ',
                'client_name ',
                'health_token_id',
            ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
        CREATE TABLE IF NOT EXISTS udw.ngs_testing_status(
            status_id INTEGER,
            health_record_key INTEGER,
            order_id STRING,
            order_type STRING,
            report_date DATE,
            specimen_collection_date DATE,
            specimen_received_date DATE,
            test STRING,
            decision STRING,
            failure_reasons STRING,
            failure_reason STRING,
            signed_by STRING,
            client_id INTEGER,
            client_name STRING,
            health_token_id BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION ''
        """)

        # write data into delta lake practice table
        ngs_testing_status_df.write.format("delta").mode("append").save(ngs_testing_status_src_file_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()