import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_UBREV_CODE_PATH'] = 's3a://delta-lake-tables/udw/measure_ubrev_code.csv'
os.environ['MEASURE_UBREV_CODE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_ubrev_code'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_measure_ubrev_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_ubrev_code_src_file_path = os.environ.get("UDW_MEASURE_UBREV_CODE_PATH", "")
        if not measure_ubrev_code_src_file_path:
            exit_with_error(logger, f"measure_ubrev_code source file path should be provided!")

        measure_ubrev_code_delta_table_path = os.environ.get("MEASURE_UBREV_CODE_DELTA_TABLE_PATH", "")
        if not measure_ubrev_code_delta_table_path:
            exit_with_error(logger, f"measure_ubrev_code delta table path should be provided!")

        # read measure_ubrev_code source csv file from s3 bucket
        logger.warn(f"read measure_ubrev_code source csv file from s3")
        src_df = spark.read.csv(measure_ubrev_code_src_file_path, header=True)
        measure_ubrev_code_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("code", get_column(src_df, "CODE", StringType())) \
            .withColumn("descr", get_column(src_df, "DESCR", StringType())) \
            .withColumn("admission_date", get_column(src_df, "ADMISSION_DATE", DateType())) \
            .withColumn("discharge_date", get_column(src_df, "DISCHARGE_DATE", DateType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("dob", get_column(src_df, "DOB", DateType())) \
            .withColumn("claim_key", get_column(src_df, "CLAIMKEY", IntegerType())) \
            .withColumn("bill_class_id", get_column(src_df, "BILLCLASSID", IntegerType())) \
            .withColumn("claim_type", get_column(src_df, "claim_type", StringType())) \
            .withColumn("rendering_npi", get_column(src_df, "RENDERING_NPI", StringType())) \
            .withColumn("rendering_first_name", get_column(src_df, "RENDERING_FIRSTNAME", StringType())) \
            .withColumn("rendering_last_name", get_column(src_df, "RENDERING_LASTNAME", StringType())) \
            .withColumn("pat_discharge", get_column(src_df, "PAT_DISCHARGE", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("rendering_provider_id", get_column(src_df, "RenderingProviderID", LongType())) \
            .select([
                "id",
                "health_record_key",
                "code",
                "descr",
                "admission_date",
                "discharge_date",
                "hie_source_code",
                "dob",
                "claim_key",
                "bill_class_id",
                "claim_type",
                "rendering_npi",
                "rendering_first_name",
                "rendering_last_name",
                "pat_discharge",
                "client_id",
                "client_name",
                "health_token_id",
                "target_created_date",
                "hash_bytes_sha",
                "rendering_provider_id"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_ubrev_code (
                id INTEGER,
                health_record_key INTEGER,
                code STRING,
                descr STRING,
                admission_date DATE,
                discharge_date DATE,
                hie_source_code STRING,
                dob DATE,
                claim_key INTEGER,
                bill_class_id INTEGER,
                claim_type STRING,
                rendering_npi STRING,
                rendering_first_name STRING,
                rendering_last_name STRING,
                pat_discharge STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                rendering_provider_id LONG
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_ubrev_code'
        """)

        # write data into delta lake practice table
        measure_ubrev_code_df.write.format("delta").mode("append").save(measure_ubrev_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
