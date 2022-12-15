import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_ENCOUNTER_PHYSICIAN_PROFILE_PATH'] = 's3a://delta-lake-tables/udw/encounter_physician_profile.csv'
os.environ['ENCOUNTER_PHYSICIAN_PROFILE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/encounter_physician_profile'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_encounter_physician_profile_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        encounter_physician_profile_src_file_path = os.environ.get("UDW_ENCOUNTER_PHYSICIAN_PROFILE_PATH", "")
        if not encounter_physician_profile_src_file_path:
            exit_with_error(logger, f"encounter_physician_profile source file path should be provided!")

        encounter_physician_profile_delta_table_path = os.environ.get("ENCOUNTER_PHYSICIAN_PROFILE_DELTA_TABLE_PATH", "")
        if not encounter_physician_profile_delta_table_path:
            exit_with_error(logger, f"encounter_physician_profile delta table path should be provided!")

        # read encounter_physician_profile source csv file from s3 bucket
        logger.warn(f"read encounter_physician_profile source csv file from s3")
        src_df = spark.read.csv(encounter_physician_profile_src_file_path, header=True)
        encounter_physician_profile_df = src_df \
            .withColumn("hospitalization_id", get_column(src_df, "HospitalizationID", LongType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", LongType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("provider_npi", get_column(src_df, "ProviderNPI", StringType())) \
            .withColumn("provider_id", get_column(src_df, "ProviderID", LongType())) \
            .withColumn("physician_type", get_column(src_df, "PhysicianType", StringType())) \
            .withColumn("first_name", get_column(src_df, "FirstName", StringType())) \
            .withColumn("middle_initial", get_column(src_df, "MiddleInitial", StringType())) \
            .withColumn("last_name", get_column(src_df, "LastName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashbytesSHA", BinaryType())) \
            .select([
                "hospitalization_id",
                "health_record_key",
                "health_token_id",
                "provider_npi",
                "provider_id",
                "physician_type",
                "first_name",
                "middle_initial",
                "last_name",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.encounter_physician_profile (
                hospitalization_id LONG,
                health_record_key LONG,
                health_token_id BINARY,
                provider_npi STRING,
                provider_id LONG,
                physician_type STRING,
                first_name STRING,
                middle_initial STRING,
                last_name STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/encounter_physician_profile'
        """)

        # write data into delta lake encounter_physician_profile table
        encounter_physician_profile_df.write.format("delta").mode("append").save(encounter_physician_profile_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
