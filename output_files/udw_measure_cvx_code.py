import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_MEASURE_CVX_CODE_PATH'] = 's3a://delta-lake-tables/udw/measure_cvx_code.csv'
os.environ['MEASURE_CVX_CODE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/measure_cvx_code'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_measure_cvx_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_cvx_code_src_file_path = os.environ.get("UDW_MEASURE_CVX_CODE_PATH", "")
        if not measure_cvx_code_src_file_path:
            exit_with_error(logger, f"measure_cvx_code source file path should be provided!")

        measure_cvx_code_delta_table_path = os.environ.get("MEASURE_CVX_CODE_DELTA_TABLE_PATH", "")
        if not measure_cvx_code_delta_table_path:
            exit_with_error(logger, f"measure_cvx_code delta table path should be provided!")

        # read measure_cvx_code source csv file from s3 bucket
        logger.warn(f"read measure_cvx_code source csv file from s3")
        src_df = spark.read.csv(measure_cvx_code_src_file_path, header=True)
        measure_cvx_code_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("health_record_key", get_column(src_df, "HEALTHRECORDKEY", IntegerType())) \
            .withColumn("immunization_code", get_column(src_df, "IMMUNIZATION_CODE", StringType())) \
            .withColumn("immunization_desc", get_column(src_df, "IMMUNIZATION_DESC", StringType())) \
            .withColumn("from_date", get_column(src_df, "FromDate", DateType())) \
            .withColumn("to_date", get_column(src_df, "TODATE", DateType())) \
            .withColumn("immunization_start_date", get_column(src_df, "IMMUNIZATION_STARTDATE", DateType())) \
            .withColumn("immunization_end_date", get_column(src_df, "IMMUNIZATION_ENDDATE", DateType())) \
            .withColumn("hie_source_code", get_column(src_df, "HIE_SOURCE_CODE", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "id",
                "health_record_key",
                "immunization_code",
                "immunization_desc",
                "from_date",
                "to_date",
                "immunization_start_date",
                "immunization_end_date",
                "hie_source_code",
                "client_id",
                "client_name",
                "health_token_id",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_cvx_code (
                id INTEGER,
                health_record_key INTEGER,
                immunization_code STRING,
                immunization_desc STRING,
                from_date DATE,
                to_date DATE,
                immunization_start_date DATE,
                immunization_end_date DATE,
                hie_source_code STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/measure_cvx_code'
        """)

        # write data into delta lake facility table
        measure_cvx_code_df.write.format("delta").mode("append").save(measure_cvx_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
