import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_BMI_CODE_PATH'] = 's3a://delta-lake-tables/udw/measure_bmi_code.csv'
os.environ['MEASURE_BMI_CODE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_bmi_code'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_measure_bmi_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_bmi_code_src_file_path = os.environ.get("UDW_MEASURE_BMI_CODE_PATH", "")
        if not measure_bmi_code_src_file_path:
            exit_with_error(logger, f"measure_bmi_code source file path should be provided!")

        measure_bmi_code_delta_table_path = os.environ.get("MEASURE_BMI_CODE_DELTA_TABLE_PATH", "")
        if not measure_bmi_code_delta_table_path:
            exit_with_error(logger, f"measure_bmi_code delta table path should be provided!")

        # read measure_bmi_code source csv file from s3 bucket
        logger.warn(f"read measure_bmi_code source csv file from s3")
        src_df = spark.read.csv(measure_bmi_code_src_file_path, header=True)
        measure_bmi_code_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("bmi", get_column(src_df, "BMI", StringType())) \
            .withColumn("bmi_date", get_column(src_df, "BMIDATE", DateType())) \
            .withColumn("follow_up", get_column(src_df, "Follow_up", StringType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("weight", get_column(src_df, "Weight", StringType())) \
            .withColumn("height", get_column(src_df, "Height", StringType())) \
            .withColumn("weight_units", get_column(src_df, "WEIGHT_UNITS", StringType())) \
            .withColumn("height_units", get_column(src_df, "HEIGHT_UNITS", StringType())) \
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
                "bmi",
                "bmi_date",
                "follow_up",
                "hie_source_code",
                "weight",
                "height",
                "weight_units",
                "height_units",
                "client_id",
                "client_name",
                "health_token_id",
                "target_created_date",
                "hash_bytes_sha",
                "rendering_npi",
                "rendering_provider_id",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_bmi_code (
                id INTEGER,
                health_record_key INTEGER,
                bmi STRING,
                bmi_date DATE,
                follow_up STRING,
                hie_source_code STRING,
                weight STRING,
                height STRING,
                weight_units STRING,
                height_units STRING,
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
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_bmi_code'
        """)

        # write data into delta lake measure_bmi_code_df table
        measure_bmi_code_df.write.format("delta").mode("append").save(measure_bmi_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
