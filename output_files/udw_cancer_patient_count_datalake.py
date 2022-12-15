import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)
os.environ['UDW_CANCER_PATIENT_COUNT_DATA_LAKE_PATH'] = 's3a://delta-lake-tables/udw/cancer_patient_count_data_lake.csv'
os.environ['CANCER_PATIENT_COUNT_DATA_LAKE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/cancer_patient_count_data_lake'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_cancer_patient_count_data_lake_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        cancer_patient_count_data_lake_src_file_path = os.environ.get("UDW_CANCER_PATIENT_COUNT_DATA_LAKE_PATH", "")
        if not cancer_patient_count_data_lake_src_file_path:
            exit_with_error(logger, f"cancer_patient_count_datalake source file path should be provided!")

        cancer_patient_count_data_lake_delta_table_path = os.environ.get("CANCER_PATIENT_COUNT_DATA_LAKE_DELTA_TABLE_PATH", "")
        if not cancer_patient_count_data_lake_delta_table_path:
            exit_with_error(logger, f"cancer_patient_count_datalake delta table path should be provided!")

        # read cancer_patient_count_datalake source csv file from s3 bucket
        logger.warn(f"read cancer_patient_count_datalake source csv file from s3")
        src_df = spark.read.csv(cancer_patient_count_data_lake_src_file_path, header=True)
        cancer_patient_count_data_lake_df = src_df \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("diag_code", get_column(src_df, "DiagCode", StringType())) \
            .withColumn("diag_group_desc", get_column(src_df, "DiagGroupDesc", StringType())) \
            .withColumn("cancer_bundle", get_column(src_df, "CancerBundle", StringType())) \
            .withColumn("icd10_code", get_column(src_df, "ICD10 Code", StringType())) \
            .withColumn("zip", get_column(src_df, "zip", StringType())) \
            .select([
                "client_id",
                "client_name",
                "health_token_id",
                "diag_code",
                "diag_group_desc",
                "cancer_bundle",
                "icd10_code",
                "zip"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.cancer_patient_count_data_lake (
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY,
                diag_code STRING,
                diag_group_desc STRING,
                cancer_bundle STRING,
                icd10_code STRING,
                zip STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/cancer_patient_count_data_lake'
        """)

        # write data into delta lake cancer_patient_count_datalake table
        cancer_patient_count_data_lake_df.write.format("delta").mode("append").save(cancer_patient_count_data_lake_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
