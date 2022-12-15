import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_PRACTICE_PATH'] = 's3a://delta-lake-tables/udw/practice.csv'
os.environ['PRACTICE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/practice'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_practice_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        practice_src_file_path = os.environ.get("UDW_PRACTICE_PATH", "")
        if not practice_src_file_path:
            exit_with_error(logger, f"practice source file path should be provided!")

        practice_delta_table_path = os.environ.get("PRACTICE_DELTA_TABLE_PATH", "")
        if not practice_delta_table_path:
            exit_with_error(logger, f"practice delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read practice source csv file from s3")
        src_df = spark.read.csv(practice_src_file_path, header=True)
        practice_df = src_df \
            .withColumn("target_practice_id", get_column(src_df, "TargetPracticeID", LongType())) \
            .withColumn("source_practice_id", get_column(src_df, "SourcePracticeID", LongType())) \
            .withColumn("fed_tax_id", get_column(src_df, "FedTaxID", StringType())) \
            .withColumn("practice_name", get_column(src_df, "PracticeName", StringType())) \
            .withColumn("practice_address", get_column(src_df, "PracticeAddress", StringType())) \
            .withColumn("practice_state_name", get_column(src_df, "PracticeStateName", StringType())) \
            .withColumn("practice_city", get_column(src_df, "PracticeCity", StringType())) \
            .withColumn("practice_zip", get_column(src_df, "PracticeZip", StringType())) \
            .withColumn("practice_contact", get_column(src_df, "PracticeContact", StringType())) \
            .withColumn("practice_billing_name", get_column(src_df, "PracticeBillingName", StringType())) \
            .withColumn("group_npi", get_column(src_df, "GroupNPI", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "target_practice_id",
                "source_practice_id",
                "fed_tax_id",
                "practice_name",
                "practice_address",
                "practice_state_name",
                "practice_city",
                "practice_zip",
                "practice_contact",
                "practice_billing_name",
                "group_npi",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.practice (
                target_practice_id LONG,
                source_practice_id LONG,
                fed_tax_id STRING,
                practice_name STRING,
                practice_address STRING,
                practice_state_name STRING,
                practice_city STRING,
                practice_zip STRING,
                practice_contact STRING,
                practice_billing_name STRING,
                group_npi STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/practice'
        """)

        # write data into delta lake practice table
        practice_df.write.format("delta").mode("append").save(practice_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
