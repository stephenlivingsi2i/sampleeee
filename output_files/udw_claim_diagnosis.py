import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_CLAIM_DIAGNOSIS'] = 's3a://delta-lake-tables/udw/claim_diagnosis.csv'
os.environ['CLAIM_DIAGNOSIS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/claim_diagnosis'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_claim_diagnosis_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        claim_diagnosis_src_file_path = os.environ.get("UDW_CLAIM_DIAGNOSIS", "")
        if not claim_diagnosis_src_file_path:
            exit_with_error(logger, f"claim_diagnosis source file path should be provided!")

        claim_diagnosis_delta_table_path = os.environ.get("CLAIM_DIAGNOSIS_DELTA_TABLE_PATH", "")
        if not claim_diagnosis_delta_table_path:
            exit_with_error(logger, f"claim_diagnosis delta table path should be provided!")

        # read claim_diagnosis source csv file from s3 bucket
        logger.warn(f"read claim_diagnosis source csv file from s3")
        src_df = spark.read.csv(claim_diagnosis_src_file_path, header=True)
        claim_diagnosis_df = src_df \
            .withColumn("claim_key", get_column(src_df, "ClaimKey", LongType())) \
            .withColumn("diag_identifier", get_column(src_df, "DiagIdentifier", StringType())) \
            .withColumn("diag_code", get_column(src_df, "DiagCode", StringType())) \
            .withColumn("diag_desc", get_column(src_df, "DiagDesc", StringType())) \
            .withColumn("diag_group_desc", get_column(src_df, "DiagGroupDesc", StringType())) \
            .withColumn("icd_version", get_column(src_df, "ICDVersion", StringType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", LongType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "claim_key",
                "diag_identifier",
                "diag_code",
                "diag_desc",
                "diag_group_desc",
                "icd_version",
                "health_record_key",
                "health_token_id",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.claim_diagnosis (
                claim_key LONG,
                diag_identifier STRING,
                diag_code STRING,
                diag_desc STRING,
                diag_group_desc STRING,
                icd_version STRING,
                health_record_key LONG,
                health_token_id BINARY,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/claim_diagnosis'
        """)

        # write data into delta lake claim_diagnosis table
        claim_diagnosis_df.write.format("delta").mode("append").save(claim_diagnosis_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
