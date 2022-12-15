import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_PRACTICE_PROVIDER_PATH'] = 's3a://delta-lake-tables/udw/practice_provider.csv'
os.environ['PRACTICE_PROVIDER_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/practice_provider'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_practice_provider_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        practice_provider_src_file_path = os.environ.get("UDW_PRACTICE_PROVIDER_PATH", "")
        if not practice_provider_src_file_path:
            exit_with_error(logger, f"practice_provider source file path should be provided!")

        practice_provider_delta_table_path = os.environ.get("PRACTICE_PROVIDER_DELTA_TABLE_PATH", "")
        if not practice_provider_delta_table_path:
            exit_with_error(logger, f"practice_provider delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read practice_provider source csv file from s3")
        src_df = spark.read.csv(practice_provider_src_file_path, header=True)
        practice_provider_df = src_df \
            .withColumn("provider_npi", get_column(src_df, "ProviderNPI", StringType())) \
            .withColumn("target_provider_id", get_column(src_df, "TargetProviderID", LongType())) \
            .withColumn("group_npi", get_column(src_df, "GroupNPI", StringType())) \
            .withColumn("target_practice_id", get_column(src_df, "TargetPracticeID", LongType())) \
            .withColumn("sub_group_id", get_column(src_df, "SubGroupID", LongType())) \
            .withColumn("segment_type", get_column(src_df, "SegmentType", IntegerType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "provider_npi",
                "target_provider_id",
                "group_npi",
                "target_practice_id",
                "sub_group_id",
                "segment_type",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.practice_provider (
                provider_npi STRING,
                target_provider_id LONG,
                group_npi STRING,
                target_practice_id LONG,
                sub_group_id LONG,
                segment_type INTEGER,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/practice_provider'
        """)

        # write data into delta lake practice table
        practice_provider_df.write.format("delta").mode("append").save(practice_provider_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
