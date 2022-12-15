import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_NGS_ORDER_DETAILS_INFO_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/NGS_ORDER_DETAILS_INFO.csv"
# export NGS_ORDER_DETAILS_INFO_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/NGS_ORDER_DETAILS_INFO"


def main():
    app_name = "udw_ngs_order_details_info_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ngs_order_details_info_src_file_path = os.environ.get("UDW_ngs_order_details_info_FILE_PATH", "")
        if not ngs_order_details_info_src_file_path:
            exit_with_error(logger, f"ngs_order_details_info source file path should be provided!")

        ngs_order_details_info_delta_table_path = os.environ.get("ngs_order_details_info_DELTA_TABLE_PATH", "")
        if not ngs_order_details_info_delta_table_path:
            exit_with_error(logger, f"ngs_order_details_info delta table path should be provided!")

        # read ngs_order_details_info source csv file from s3 bucket
        logger.warn(f"read ngs_order_details_info source csv file from s3")
        src_df = spark.read.csv(ngs_order_details_info_src_file_path, header=True)
        
        ngs_order_details_info_df = src_df \
                .withColumn('order_info_id ', get_column(src_df,'order_info_id', IntegerType()))\
                .withColumn('health_record_key ', get_column(src_df,'HealthRecordKey', IntegerType()))\
                .withColumn('order_id ', get_column(src_df,'order_id'))\
                .withColumn('order_type ', get_column(src_df,'order_type'))\
                .withColumn('report_date ', get_column(src_df,'report_date', DateType()))\
                .withColumn('signed_by ', get_column(src_df,'signed_by'))\
                .withColumn('order_report_version ', get_column(src_df,'order_report_version', IntegerType()))\
                .withColumn('specimen_accessioned_id ', get_column(src_df,'specimen_accessioned_id'))\
                .withColumn('specimen_collection_date ', get_column(src_df,'specimen_collection_date', DateType()))\
                .withColumn('specimen_received_date ', get_column(src_df,'specimen_received_date', DateType()))\
                .withColumn('specimen_sample_type ', get_column(src_df,'specimen_sample_type'))\
                .withColumn('physician_first_name ', get_column(src_df,'physician_first_name'))\
                .withColumn('physician_last_name ', get_column(src_df,'physician_last_name'))\
                .withColumn('physician_npi ', get_column(src_df,'physician_npi', IntegerType()))\
                .withColumn('physician_ordering_facility ', get_column(src_df,'physician_ordering_facility'))\
                .withColumn('report_disease_name ', get_column(src_df,'report_disease_name'))\
                .withColumn('primary_icd_code ', get_column(src_df,'primary_icd_code'))\
                .withColumn('primary_icd_description ', get_column(src_df,'primary_icd_description'))\
                .withColumn('secondary_icd_code ', get_column(src_df,'secondary_icd_code'))\
                .withColumn('secondary_icd_description ', get_column(src_df,'secondary_icd_description'))\
                .withColumn('tumor_stage ', get_column(src_df,'tumor_stage'))\
                .withColumn('histology_full_description ', get_column(src_df,'histology_full_description'))\
                .withColumn('tissue_site_full_description ', get_column(src_df,'tissue_site_full_description'))\
                .withColumn('cancer_type ', get_column(src_df,'cancer_type'))\
                .withColumn('is_cancelled ', get_column(src_df,'isCancelled'))\
                .withColumn('client_id ', get_column(src_df,'ClientID', IntegerType()))\
                .withColumn('client_name ', get_column(src_df,'ClientName'))\
                .withColumn('health_token_id ', get_column(src_df,'HealthTokenID', BinaryType())) \
                .select([
                'order_info_id ',
                'health_record_key ',
                'order_id ',
                'order_type ',
                'report_date ',
                'signed_by ',
                'order_report_version ',
                'specimen_accessioned_id ',
                'specimen_collection_date ',
                'specimen_received_date ',
                'specimen_sample_type ',
                'physician_first_name ',
                'physician_last_name ',
                'physician_npi ',
                'physician_ordering_facility ',
                'report_disease_name ',
                'primary_icd_code ',
                'primary_icd_description ',
                'secondary_icd_code ',
                'secondary_icd_description ',
                'tumor_stage ',
                'histology_full_description ',
                'tissue_site_full_description ',
                'cancer_type ',
                'is_cancelled ',
                'client_id ',
                'client_name ',
                'health_token_id '
                ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.ngs_order_details_info(
                order_info_id INTEGER,
                health_record_key INTEGER,
                order_id STRING,
                order_type STRING,
                report_date DATE,
                signed_by STRING,
                order_report_version INTEGER,
                specimen_accessioned_id STRING,
                specimen_collection_date DATE,
                specimen_received_date DATE,
                specimen_sample_type STRING,
                physician_first_name STRING,
                physician_last_name STRING,
                physician_npi INTEGER,
                physician_ordering_facility STRING,
                report_disease_name STRING,
                primary_icd_code STRING,
                primary_icd_description STRING,
                secondary_icd_code STRING,
                secondary_icd_description STRING,
                tumor_stage STRING,
                histology_full_description STRING,
                tissue_site_full_description STRING,
                cancer_type STRING,
                is_cancelled STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION ''
        """)

        # write data into delta lake ngs_order_details_info table
        ngs_order_details_info_df.write.format("delta").mode("append").save(ngs_order_details_info_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()