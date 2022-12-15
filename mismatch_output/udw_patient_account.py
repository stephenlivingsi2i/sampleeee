import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_PATIENT_ACCOUNT_PATH'] = 's3a://delta-lake-tables/udw/patient_account.csv'
os.environ['PATIENT_ACCOUNT_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/patient_account'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_patient_account_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        patient_account_src_file_path = os.environ.get("UDW_PATIENT_ACCOUNT_PATH", "")
        if not patient_account_src_file_path:
            exit_with_error(logger, f"patient_account source file path should be provided!")

        patient_account_delta_table_path = os.environ.get("PATIENT_ACCOUNT_DELTA_TABLE_PATH", "")
        if not patient_account_delta_table_path:
            exit_with_error(logger, f"patient_account delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read patient_account source csv file from s3")
        src_df = spark.read.csv(patient_account_src_file_path, header=True)
        patient_account_df = src_df \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", LongType())) \
            .withColumn("sub_group_id", get_column(src_df, "SubGroupID", IntegerType())) \
            .withColumn("sub_group_name", get_column(src_df, "SubGroupName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashbytesSHA", BinaryType())) \
            .select([
                "health_token_id",
                "health_record_key",
                "sub_group_id",
                "sub_group_name",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.patient_account (
                health_token_id BINARY,
                health_record_key LONG,
                sub_group_id INTEGER,
                sub_group_name STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/patient_account'
        """)

        # write data into delta lake practice table
        patient_account_df.write.format("delta").mode("append").save(patient_account_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
