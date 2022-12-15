import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_MEASURE_UBREV_CODE_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/MEASURE_UBREV_CODE.csv"
# export MEASURE_UBREV_CODE_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/MEASURE_UBREV_CODE"


def main():
    app_name = "udw_measure_ubrev_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_ubrev_code_src_file_path = os.environ.get("UDW_MEASURE_UBREV_CODE_FILE_PATH", "")
        if not measure_ubrev_code_src_file_path:
            exit_with_error(logger, f"measure_ubrev_code source file path should be provided!")

        measure_ubrev_code_delta_table_path = os.environ.get("MEASURE_UBREV_CODE_DELTA_TABLE_PATH", "")
        if not measure_ubrev_code_delta_table_path:
            exit_with_error(logger, f"measure_ubrev_code delta table path should be provided!")

        # read measure_ubrev_code source csv file from s3 bucket
        logger.warn(f"read measure_ubrev_code source csv file from s3")
        src_df = spark.read.csv(measure_ubrev_code_src_file_path, header=True)
        
        measure_ubrev_code_df = src_df \
            .withColumn('id ', get_column(src_df,'ID', IntegerType()))\
            .withColumn('health_record_key ', get_column(src_df,'HEALTHRECORDKEY', IntegerType()))\
            .withColumn('code ', get_column(src_df,'CODE'))\
            .withColumn('descr ', get_column(src_df,'DESCR'))\
            .withColumn('admission_date ', get_column(src_df,'ADMISSION_DATE', DateType()))\
            .withColumn('discharge_date ', get_column(src_df,'DISCHARGE_DATE', DateType()))\
            .withColumn('hie_source_code ', get_column(src_df,'HIE_SOURCE_CODE'))\
            .withColumn('dob ', get_column(src_df,'DOB', DateType()))\
            .withColumn('claim_key ', get_column(src_df,'CLAIMKEY', IntegerType()))\
            .withColumn('bill_class_id ', get_column(src_df,'BILLCLASSID', IntegerType()))\
            .withColumn('claim_type ', get_column(src_df,'claim_type'))\
            .withColumn('rendering_npi ', get_column(src_df,'RENDERING_NPI'))\
            .withColumn('rendering_first_name ', get_column(src_df,'RENDERING_FIRSTNAME'))\
            .withColumn('rendering_last_name ', get_column(src_df,'RENDERING_LASTNAME'))\
            .withColumn('pat_discharge ', get_column(src_df,'PAT_DISCHARGE'))\
            .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
            .withColumn('client_name ', get_column(src_df,'ClientName'))\
            .withColumn('health_token_id ', get_column(src_df,'HealthTokenID', BinaryType()))\
            .withColumn('target_created_date ', get_column(src_df,'TargetCreatedDate', TimestampType()))\
            .withColumn('hash_bytes_sha ', get_column(src_df,'HashBytesSHA', BinaryType()))\
            .withColumn('rendering_provider_id', get_column(src_df,'RenderingProviderID', IntegerType()))\
            .select([
                'id ',
                'health_record_key ',
                'code ',
                'descr ',
                'admission_date ',
                'discharge_date ',
                'hie_source_code ',
                'dob ',
                'claim_key ',
                'bill_class_id ',
                'claim_type ',
                'rendering_npi ',
                'rendering_first_name ',
                'rendering_last_name ',
                'pat_discharge ',
                'client_id ',
                'client_name ',
                'health_token_id ',
                'target_created_date ',
                'hash_bytes_sha ',
                'rendering_provider_id',
            ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_ubrev_code(
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
                rendering_provider_id INTEGER
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION ''
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