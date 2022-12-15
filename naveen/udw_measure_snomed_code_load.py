import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_MEASURE_SNOMED_CODE_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/MEASURE_SNOMED_CODE.csv"
# export MEASURE_SNOMED_CODE_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/MEASURE_SNOMED_CODE"


def main():
    app_name = "udw_measure_snomed_code_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        measure_snomed_code_src_file_path = os.environ.get("UDW_MEASURE_SNOMED_CODE_FILE_PATH", "")
        if not measure_snomed_code_src_file_path:
            exit_with_error(logger, f"measure_snomed_code source file path should be provided!")

        measure_snomed_code_delta_table_path = os.environ.get("MEASURE_SNOMED_CODE_DELTA_TABLE_PATH", "")
        if not measure_snomed_code_delta_table_path:
            exit_with_error(logger, f"measure_snomed_code delta table path should be provided!")

        # read measure_snomed_code source csv file from s3 bucket
        logger.warn(f"read measure_snomed_code source csv file from s3")
        src_df = spark.read.csv(measure_snomed_code_src_file_path, header=True)
        
        measure_snomed_code_df = src_df \
            .withColumn('id ', get_column(src_df,'ID', IntegerType()))\
            .withColumn('health_record_key ', get_column(src_df,'HEALTHRECORDKEY', IntegerType()))\
            .withColumn('snomed ', get_column(src_df,'SNOMED'))\
            .withColumn('snomed_desc ', get_column(src_df,'SNOMED_desc'))\
            .withColumn('v_date ', get_column(src_df,'V_DATE', DateType()))\
            .withColumn('to_date ', get_column(src_df,'TO_DATE', DateType()))\
            .withColumn('snomed_start_date ', get_column(src_df,'SNOMED_STARTDATE', DateType()))\
            .withColumn('snomed_end_date ', get_column(src_df,'SNOMED_ENDDATE', DateType()))\
            .withColumn('hie_source_code ', get_column(src_df,'HIE_SOURCE_CODE'))\
            .withColumn('rendering_npi ', get_column(src_df,'RENDERING_NPI'))\
            .withColumn('observation_code ', get_column(src_df,'OBSERVATIONCODE'))\
            .withColumn('observation_code_system ', get_column(src_df,'OBSERVATIONCODESYSTEM'))\
            .withColumn('observation_display_name ', get_column(src_df,'OBSERVATIONDISPLAYNAME'))\
            .withColumn('added_date ', get_column(src_df,'ADDED_DATE', DateType()))\
            .withColumn('is_parent_diag ', get_column(src_df,'ISPARENTDIAG'))\
            .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
            .withColumn('client_name ', get_column(src_df,'ClientName'))\
            .withColumn('health_token_id ', get_column(src_df,'HealthTokenID', BinaryType()))\
            .withColumn('target_created_date ', get_column(src_df,'TargetCreatedDate', TimestampType()))\
            .withColumn('hash_bytes_sha ', get_column(src_df,'HashBytesSHA', BinaryType()))\
            .withColumn('rendering_provider_id ', get_column(src_df,'RenderingProviderID', IntegerType()))\
            .select([
                'id ',
                'health_record_key ',
                'snomed ',
                'snomed_desc ',
                'v_date ',
                'to_date ',
                'snomed_start_date ',
                'snomed_end_date ',
                'hie_source_code ',
                'rendering_npi ',
                'observation_code ',
                'observation_code_system ',
                'observation_display_name ',
                'added_date ',
                'is_parent_diag ',
                'client_id ',
                'client_name ',
                'health_token_id ',
                'target_created_date ',
                'hash_bytes_sha ',
                'rendering_provider_id ',
            ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.measure_snomed_code(
                id INTEGER,
                health_record_key INTEGER,
                snomed STRING,
                snomed_desc STRING,
                v_date DATE,
                to_date DATE,
                snomed_start_date DATE,
                snomed_end_date DATE,
                hie_source_code STRING,
                rendering_npi STRING,
                observation_code STRING,
                observation_code_system STRING,
                observation_display_name STRING,
                added_date DATE,
                is_parent_diag STRING,
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
        measure_snomed_code_df.write.format("delta").mode("append").save(measure_snomed_code_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()