import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_ICD_CODE_BCP_PATH'] = 's3a://delta-lake-tables/udw/measure_icd_code_bcp.csv'
os.environ['MEASURE_ICD_CODE_BCP_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_icd_code_bcp'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_measure_icd_code_bcp_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_icd_code_bcp_src_file_path = os.environ.get("UDW_MEASURE_ICD_CODE_BCP_PATH", "")
        if not measure_icd_code_bcp_src_file_path:
            exit_with_error(logger, f"measure_icd_code source file path should be provided!")

        measure_icd_code_bcp_delta_table_path = os.environ.get("MEASURE_ICD_CODE_BCP_DELTA_TABLE_PATH", "")
        if not measure_icd_code_bcp_delta_table_path:
            exit_with_error(logger, f"measure_icd_code_bcp delta table path should be provided!")

        # read measure_icd_code_bcp source csv file from s3 bucket
        logger.warn(f"read measure_icd_code_bcp source csv file from s3")
        src_df = spark.read.csv(measure_icd_code_bcp_src_file_path, header=True)
        measure_icd_code_bcp_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("dx_code", get_column(src_df, "DXCODE", StringType())) \
            .withColumn("dx_descr", get_column(src_df, "DXDESCR", StringType())) \
            .withColumn("v_date", get_column(src_df, "V_DATE", DateType())) \
            .withColumn("rendering_npi", get_column(src_df, "Rendering_NPI", StringType())) \
            .withColumn("rendering_first_name", get_column(src_df, "Rendering_FirstName", StringType())) \
            .withColumn("rendering_last_name", get_column(src_df, "Rendering_LastName", StringType())) \
            .withColumn("diagnosis_date", get_column(src_df, "DIAGNOSIS_DATE", DateType())) \
            .withColumn("to_date", get_column(src_df, "TO_DATE", DateType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("is_parent_diag", get_column(src_df, "isParentdiag", StringType())) \
            .withColumn("added_date", get_column(src_df, "ADDED_DATE", DateType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("rendering_provider_id", get_column(src_df, "RenderingProviderID", LongType())) \
            .select([
                "id",
                "health_record_key",
                "dx_code",
                "dx_descr",
                "v_date",
                "rendering_npi",
                "rendering_first_name",
                "rendering_last_name",
                "diagnosis_date",
                "to_date",
                "hie_source_code",
                "is_parent_diag",
                "added_date",
                "client_id",
                "client_name",
                "health_token_id",
                "target_created_date",
                "hash_bytes_sha",
                "rendering_provider_id",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_icd_code_bcp (
                id INTEGER,
                health_record_key INTEGER,
                dx_code STRING,
                dx_descr STRING,
                v_date DATE,
                rendering_npi STRING,
                rendering_first_name STRING,
                rendering_last_name STRING,
                diagnosis_date DATE,
                to_date DATE,
                hie_source_code STRING,
                is_parent_diag STRING,
                added_date DATE,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                rendering_provider_id LONG
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_icd_code_bcp'
        """)

        # write data into delta lake measure_icd_code_bcp table
        measure_icd_code_bcp_df.write.format("delta").mode("append").save(measure_icd_code_bcp_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
