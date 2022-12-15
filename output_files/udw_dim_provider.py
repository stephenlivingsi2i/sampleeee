import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_DIM_PROVIDER_PATH'] = 's3a://delta-lake-tables/udw/dim_provider.csv'
os.environ['DIM_PROVIDER_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/dim_provider'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_dim_provider_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        dim_provider_src_file_path = os.environ.get("UDW_DIM_PROVIDER_PATH", "")
        if not dim_provider_src_file_path:
            exit_with_error(logger, f"dim_provider source file path should be provided!")

        dim_provider_delta_table_path = os.environ.get("DIM_PROVIDER_DELTA_TABLE_PATH", "")
        if not dim_provider_delta_table_path:
            exit_with_error(logger, f"dim_provider delta table path should be provided!")

        # read dim_provider source csv file from s3 bucket
        logger.warn(f"read dim_provider source csv file from s3")
        src_df = spark.read.csv(dim_provider_src_file_path, header=True)
        dim_provider_df = src_df \
            .withColumn("provider_key", get_column(src_df, "ProviderKey", LongType())) \
            .withColumn("provider_token_id", get_column(src_df, "ProviderTokenID", BinaryType())) \
            .withColumn("first_name", get_column(src_df, "FirstName", StringType())) \
            .withColumn("last_name", get_column(src_df, "LastName", StringType())) \
            .withColumn("national_provider_id", get_column(src_df, "NationalProviderID", StringType())) \
            .withColumn("speciality_desc", get_column(src_df, "SpecialityDesc", StringType())) \
            .withColumn("address1", get_column(src_df, "Address1", StringType())) \
            .withColumn("city", get_column(src_df, "City", StringType())) \
            .withColumn("state", get_column(src_df, "State", StringType())) \
            .withColumn("zip", get_column(src_df, "Zip", StringType())) \
            .withColumn("contact_number", get_column(src_df, "ContactNumber", StringType())) \
            .withColumn("bh_specialty", get_column(src_df, "BHspecialty", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashbytesSHA", BinaryType())) \
            .withColumn("d_first_name", get_column(src_df, "DFirstName", StringType())) \
            .withColumn("d_last_name", get_column(src_df, "DLastName", StringType())) \
            .withColumn("dnpi", get_column(src_df, "DNPI", StringType())) \
            .select([
                "provider_key",
                "provider_token_id",
                "first_name",
                "last_name",
                "national_provider_id",
                "speciality_desc",
                "address1",
                "city",
                "state",
                "zip",
                "contact_number",
                "bh_specialty",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
                "d_first_name",
                "d_last_name",
                "dnpi",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.dim_provider (
                provider_key LONG,
                provider_token_id BINARY,
                first_name STRING,
                last_name STRING,
                national_provider_id STRING,
                speciality_desc STRING,
                address1 STRING,
                city STRING,
                state STRING,
                zip STRING,
                contact_number STRING,
                bh_specialty STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                d_first_name STRING,
                d_last_name STRING,
                dnpi STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/dim_provider'
        """)

        # write data into delta lake dim_provider table
        dim_provider_df.write.format("delta").mode("append").save(dim_provider_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
