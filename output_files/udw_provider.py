import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_PROVIDER_PATH'] = 's3a://delta-lake-tables/udw/provider.csv'
os.environ['PROVIDER_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/provider'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_provider_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        provider_src_file_path = os.environ.get("UDW_PROVIDER_PATH", "")
        if not provider_src_file_path:
            exit_with_error(logger, f"provider source file path should be provided!")

        provider_delta_table_path = os.environ.get("PROVIDER_DELTA_TABLE_PATH", "")
        if not provider_delta_table_path:
            exit_with_error(logger, f"provider delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read practice source csv file from s3")
        src_df = spark.read.csv(provider_src_file_path, header=True)
        provider_df = src_df \
            .withColumn("provider_key", get_column(src_df, "ProviderKey", LongType())) \
            .withColumn("target_provider_id", get_column(src_df, "TargetProviderID", LongType())) \
            .withColumn("first_name", get_column(src_df, "FirstName", StringType())) \
            .withColumn("last_name", get_column(src_df, "LastName", StringType())) \
            .withColumn("national_provider_id", get_column(src_df, "NationalProviderID", StringType())) \
            .withColumn("speciality_desc", get_column(src_df, "SpecialityDesc", StringType())) \
            .withColumn("provider_address", get_column(src_df, "ProviderAddress", StringType())) \
            .withColumn("provider_city", get_column(src_df, "ProviderCity", StringType())) \
            .withColumn("provider_state", get_column(src_df, "ProviderState", StringType())) \
            .withColumn("provider_zip", get_column(src_df, "ProviderZip", StringType())) \
            .withColumn("provider_contact_number", get_column(src_df, "ProviderContactNumber", StringType())) \
            .withColumn("bhs_pecialty", get_column(src_df, "BHspecialty", StringType())) \
            .withColumn("credentials", get_column(src_df, "Credentials", StringType())) \
            .withColumn("primary_specialty", get_column(src_df, "PrimarySpecialty", StringType())) \
            .withColumn("master_specialty", get_column(src_df, "MasterSpecialty", StringType())) \
            .withColumn("provider_derived_from", get_column(src_df, "ProviderDerivedFrom", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashbytesSHA", BinaryType())) \
            .select([
                "provider_key",
                "target_provider_id",
                "first_name",
                "last_name",
                "national_provider_id",
                "speciality_desc",
                "provider_address",
                "provider_city",
                "provider_state",
                "provider_zip",
                "provider_contact_number",
                "bhs_pecialty",
                "credentials",
                "primary_specialty",
                "master_specialty",
                "provider_derived_from",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.provider (
                provider_key LONG,
                target_provider_id LONG,
                first_name STRING,
                last_name STRING,
                national_provider_id STRING,
                speciality_desc STRING,
                provider_address STRING,
                provider_city STRING,
                provider_state STRING,
                provider_zip STRING,
                provider_contact_number STRING,
                bhs_pecialty STRING,
                credentials STRING,
                primary_specialty STRING,
                master_specialty STRING,
                provider_derived_from STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/provider'
        """)

        # write data into delta lake provider table
        provider_df.write.format("delta").mode("append").save(provider_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
