import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_NDC_CODE_PATH'] = 's3a://delta-lake-tables/udw/measure_ndc_code.csv'
os.environ['MEASURE_NDC_CODE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_ndc_code'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_measure_ndc_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_ndc_code_src_file_path = os.environ.get("UDW_MEASURE_NDC_CODE_PATH", "")
        if not measure_ndc_code_src_file_path:
            exit_with_error(logger, f"measure_ndc_code source file path should be provided!")

        measure_ndc_code_delta_table_path = os.environ.get("MEASURE_NDC_CODE_DELTA_TABLE_PATH", "")
        if not measure_ndc_code_delta_table_path:
            exit_with_error(logger, f"measure_ndc_code delta table path should be provided!")

        # read measure_ndc_code source csv file from s3 bucket
        logger.warn(f"read measure_ndc_code source csv file from s3")
        src_df = spark.read.csv(measure_ndc_code_src_file_path, header=True)
        measure_ndc_code_df = src_df \
            .withColumn("id", get_column(src_df, "id", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("dispense_date", get_column(src_df, "Dispense_DATE", DateType())) \
            .withColumn("med_start_date", get_column(src_df, "Med_START_DATE", DateType())) \
            .withColumn("med_end_date", get_column(src_df, "Med_End_Date", DateType())) \
            .withColumn("dispense_use", get_column(src_df, "Dispense_USE", StringType())) \
            .withColumn("ndc", get_column(src_df, "NDC", StringType())) \
            .withColumn("rxn", get_column(src_df, "rxn", StringType())) \
            .withColumn("code_desc", get_column(src_df, "CODE_DESC", StringType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("supply", get_column(src_df, "supply", StringType())) \
            .withColumn("quantity", get_column(src_df, "Quantity", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("rendering_npi", get_column(src_df, "RENDERING_NPI", StringType())) \
            .withColumn("rendering_provider_id", get_column(src_df, "RenderingProviderID", LongType())) \
            .select([
                "id",
                "health_record_key",
                "dispense_date",
                "med_start_date",
                "med_end_date",
                "dispense_use",
                "ndc",
                "rxn",
                "code_desc",
                "hie_source_code",
                "supply",
                "quantity",
                "client_id",
                "client_name",
                "health_token_id",
                "target_created_date",
                "hash_bytes_sha",
                "rendering_npi",
                "rendering_provider_id"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_ndc_code (
                id INTEGER,
                health_record_key INTEGER,
                dispense_date DATE,
                med_start_date DATE,
                med_end_date DATE,
                dispense_use STRING,
                ndc STRING,
                rxn STRING,
                code_desc STRING,
                hie_source_code STRING,
                supply STRING,
                quantity STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                rendering_npi STRING,
                rendering_provider_id LONG
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_ndc_code'
        """)

        # write data into delta lake measure_ndc_code table
        measure_ndc_code_df.write.format("delta").mode("append").save(measure_ndc_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
