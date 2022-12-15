import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)
os.environ['UDW_CANCER_STAGING_PATH'] = 's3a://delta-lake-tables/udw/cancer_staging.csv'
os.environ['CANCER_STAGING_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/cancer_staging'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_cancer_staging_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        cancer_staging_src_file_path = os.environ.get("UDW_CANCER_STAGING_PATH", "")
        if not cancer_staging_src_file_path:
            exit_with_error(logger, f"cancer_staging source file path should be provided!")

        cancer_staging_delta_table_path = os.environ.get("CANCER_STAGING_DELTA_TABLE_PATH", "")
        if not cancer_staging_delta_table_path:
            exit_with_error(logger, f"cancer_staging delta table path should be provided!")

        # read cancer_staging source csv file from s3 bucket
        logger.warn(f"read cancer_staging source csv file from s3")
        src_df = spark.read.csv(cancer_staging_src_file_path, header=True)
        cancer_staging_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("diagnosis_id", get_column(src_df, "DIAGNOSISID", StringType())) \
            .withColumn("diagnosis_code", get_column(src_df, "DIAGNOSISCODE", StringType())) \
            .withColumn("diagnosis_code_sys", get_column(src_df, "DIAGNOSISCODESYS", StringType())) \
            .withColumn("diagnosis_date", get_column(src_df, "DIAGNOSISDATE", StringType())) \
            .withColumn("stage", get_column(src_df, "STAGE", StringType())) \
            .withColumn("staging_documented_date", get_column(src_df, "Staging_Documented_Date", DateType())) \
            .withColumn("t", get_column(src_df, "T", StringType())) \
            .withColumn("n", get_column(src_df, "N", StringType())) \
            .withColumn("m", get_column(src_df, "M", StringType())) \
            .withColumn("tumor_grade", get_column(src_df, "Tumor_Grade", StringType())) \
            .withColumn("morphology", get_column(src_df, "Morphology", StringType())) \
            .withColumn("clinical_status", get_column(src_df, "CLINICAL_STATUS", StringType())) \
            .withColumn("biomarker", get_column(src_df, "Biomarker", StringType())) \
            .withColumn("test_result", get_column(src_df, "Test_Result", StringType())) \
            .withColumn("result_date", get_column(src_df, "Result_Date", DateType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .select([
                "client_name",
                "client_id",
                "health_token_id",
                "diagnosis_id",
                "diagnosis_code",
                "diagnosis_code_sys",
                "diagnosis_date",
                "stage",
                "staging_documented_date",
                "t",
                "n",
                "m",
                "tumor_grade",
                "morphology",
                "clinical_status",
                "biomarker",
                "test_result",
                "result_date",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.cancer_staging (
                client_name STRING,
                client_id INTEGER,
                health_token_id BINARY,
                diagnosis_id STRING,
                diagnosis_code STRING,
                diagnosis_code_sys STRING,
                diagnosis_date STRING,
                stage STRING,
                staging_documented_date DATE,
                t STRING,
                n STRING,
                m STRING,
                tumor_grade STRING,
                morphology STRING,
                clinical_status STRING,
                biomarker STRING,
                test_result STRING,
                result_date DATE,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/cancer_staging'
        """)

        # write data into delta lake cancer_staging table
        cancer_staging_df.write.format("delta").mode("append").save(cancer_staging_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
