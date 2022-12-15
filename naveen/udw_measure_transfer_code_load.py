import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_MEASURE_TRANSFER_CODE_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/MEASURE_TRANSFER_CODE.csv"
# export MEASURE_TRANSFER_CODE_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/MEASURE_TRANSFER_CODE"


def main():
    app_name = "udw_measure_transfer_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_transfer_code_src_file_path = os.environ.get("UDW_MEASURE_TRANSFER_CODE_FILE_PATH", "")
        if not measure_transfer_code_src_file_path:
            exit_with_error(logger, f"measure_transfer_code source file path should be provided!")

        measure_transfer_code_delta_table_path = os.environ.get("MEASURE_TRANSFER_CODE_DELTA_TABLE_PATH", "")
        if not measure_transfer_code_delta_table_path:
            exit_with_error(logger, f"measure_transfer_code delta table path should be provided!")

        # read measure_transfer_code source csv file from s3 bucket
        logger.warn(f"read measure_transfer_code source csv file from s3")
        src_df = spark.read.csv(measure_transfer_code_src_file_path, header=True)
        
        measure_transfer_code_df = src_df \
            .withColumn('id ', get_column(src_df,'ID', IntegerType()))\
            .withColumn('health_record_key ', get_column(src_df,'HEALTHRECORDKEY', IntegerType()))\
            .withColumn('discharge_date ', get_column(src_df,'DISCHARGEDATE', DateType()))\
            .withColumn('admission_type_id ', get_column(src_df,'ADMISSIONTYPEID'))\
            .withColumn('description ', get_column(src_df,'DESCRIPTION'))\
            .withColumn('hie_source_code ', get_column(src_df,'HIE_SOURCE_CODE'))\
            .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
            .withColumn('client_name ', get_column(src_df,'ClientName'))\
            .withColumn('health_token_id ', get_column(src_df,'HealthTokenID', BinaryType()))\
            .withColumn('target_created_date ', get_column(src_df,'TargetCreatedDate', TimestampType()))\
            .withColumn('hash_bytes_sha ', get_column(src_df,'HashBytesSHA', BinaryType()))\
            .withColumn('rendering_npi ', get_column(src_df,'RENDERING_NPI'))\
            .withColumn('rendering_provider_id ', get_column(src_df,'RenderingProviderID', IntegerType())) \
            .select([
                'id ',
                'health_record_key ',
                'discharge_date ',
                'admission_type_id ',
                'description ',
                'hie_source_code ',
                'client_id ',
                'client_name ',
                'health_token_id ',
                'target_created_date ',
                'hash_bytes_sha ',
                'rendering_npi ',
                'rendering_provider_id '
                ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_transfer_code(
                id INTEGER,
                health_record_key INTEGER,
                discharge_date DATE,
                admission_type_id STRING,
                description STRING,
                hie_source_code STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                target_created_date TIMESTAMP(3) WITH TIME ZONE,
                hash_bytes_sha BINARY,
                rendering_npi STRING,
                rendering_provider_id INTEGER
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION ''
        """)

        # write data into delta lake practice table
        measure_transfer_code_df.write.format("delta").mode("append").save(measure_transfer_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()