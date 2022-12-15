import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_NGS_DEMOGRAPHIC_INFO_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/NGS_DEMOGRAPHIC_INFO.csv"
# export NGS_DEMOGRAPHIC_INFO_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/NGS_DEMOGRAPHIC_INFO"


def main():
    app_name = "udw_ngs_demographic_info_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ngs_demographic_info_src_file_path = os.environ.get("UDW_NGS_DEMOGRAPHIC_INFO_FILE_PATH", "")
        if not ngs_demographic_info_src_file_path:
            exit_with_error(logger, f"ngs_demographic_info source file path should be provided!")

        ngs_demographic_info_delta_table_path = os.environ.get("NGS_DEMOGRAPHIC_INFO_DELTA_TABLE_PATH", "")
        if not ngs_demographic_info_delta_table_path:
            exit_with_error(logger, f"ngs_demographic_info delta table path should be provided!")

        # read ngs_demographic_info source csv file from s3 bucket
        logger.warn(f"read ngs_demographic_info source csv file from s3")
        src_df = spark.read.csv(ngs_demographic_info_src_file_path, header=True)
        
        ngs_demographic_info_df = src_df \
            .withColumn('demographic_id ', get_column(src_df,'Demographic_id', IntegerType()))\
            .withColumn('health_record_key ', get_column(src_df,'HealthRecordKey', IntegerType()))\
            .withColumn('patient_first_name ', get_column(src_df,'Patient_first_name'))\
            .withColumn('patient_last_name ', get_column(src_df,'Patient_last_name'))\
            .withColumn('patient_date_of_birth ', get_column(src_df,'Patient_date_of_birth', DateType()))\
            .withColumn('patient_gender ', get_column(src_df,'Patient_gender'))\
            .withColumn('patient_mrn ', get_column(src_df,'Patient_mrn'))\
            .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
            .withColumn('client_name ', get_column(src_df,'ClientName'))\
            .withColumn('health_token_id ', get_column(src_df,'HealthTokenID', BinaryType())) \
            .select([
                'demographic_id ',
                'health_record_key ',
                'patient_first_name ',
                'patient_last_name ',
                'patient_date_of_birth ',
                'patient_gender ',
                'patient_mrn ',
                'client_id ',
                'client_name ',
                'health_token_id '
            ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.ngs_demographic_info(
                demographic_id INTEGER,
                health_record_key INTEGER,
                patient_first_name STRING,
                patient_last_name STRING,
                patient_date_of_birth DATE,
                patient_gender STRING,
                patient_mrn STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY
                )
                USING delta
                PARTITIONED BY (client_id)
                LOCATION ''
                """)

        # write data into delta lake ngs_demographic_info table
        ngs_demographic_info_df.write.format("delta").mode("append").save(ngs_demographic_info_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()