import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType, StringType, LongType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_HIGH_BP_CODE_BCP_FILE_PATH'] = 's3a://delta-lake-tables/udw/measure_high_bp_code_bcp.csv'
os.environ['MEASURE_HIGH_BP_CODE_BCP_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_high_bp_code_bcp'


### Important: please export below environment variables
# export UDW_MEASURE_HIGH_BP_CODE_BCP_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/MEASURE_HIGH_BP_CODE_BCP.csv"
# export MEASURE_HIGH_BP_CODE_BCP_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/MEASURE_HIGH_BP_CODE_BCP"


def main():
    app_name = "udw_measure_high_bp_code_bcp_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_high_bp_code_bcp_src_file_path = os.environ.get("UDW_MEASURE_HIGH_BP_CODE_BCP_FILE_PATH", "")
        if not measure_high_bp_code_bcp_src_file_path:
            exit_with_error(logger, f"measure_high_bp_code_bcp source file path should be provided!")

        measure_high_bp_code_bcp_delta_table_path = os.environ.get("MEASURE_HIGH_BP_CODE_BCP_DELTA_TABLE_PATH", "")
        if not measure_high_bp_code_bcp_delta_table_path:
            exit_with_error(logger, f"measure_high_bp_code_bcp delta table path should be provided!")

        # read measure_high_bp_code_bcp source csv file from s3 bucket
        logger.warn(f"read measure_high_bp_code_bcp source csv file from s3")
        src_df = spark.read.csv(measure_high_bp_code_bcp_src_file_path, header=True)
        
        measure_high_bp_code_bcp_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("systolic", get_column(src_df, "SYSTOLIC", StringType())) \
            .withColumn("diastolic", get_column(src_df, "DIASTOLIC", StringType())) \
            .withColumn("bp_date", get_column(src_df, "BPDATE", DateType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("follow_up_flag", get_column(src_df, "Followupflag", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("rendering_npi", get_column(src_df, "RENDERING_NPI", StringType())) \
            .withColumn("rendering_provider_id", get_column(src_df, "RenderingProviderID", LongType())) \
            .select([
                "id",
                "health_record_key",
                "systolic",
                "diastolic",
                "bp_date",
                "hie_source_code",
                "follow_up_flag",
                "health_token_id",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
                "rendering_npi",
                "rendering_provider_id"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_high_bp_code_bcp (
                id INTEGER,
                health_record_key INTEGER,
                systolic STRING,
                diastolic STRING,
                bp_date DATE,
                hie_source_code STRING,
                follow_up_flag STRING,
                health_token_id BINARY,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                rendering_npi STRING,
                rendering_provider_id LONG
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_high_bp_code_bcp'
        """)

        # write data into delta lake measure_high_bp_code_bcp table
        measure_high_bp_code_bcp_df.write.format("delta").mode("append").save(measure_high_bp_code_bcp_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()