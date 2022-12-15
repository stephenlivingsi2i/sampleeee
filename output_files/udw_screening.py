import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_SCREENING_PATH'] = 's3a://delta-lake-tables/udw/screening.csv'
os.environ['SCREENING_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/screening'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_screening_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        screening_src_file_path = os.environ.get("UDW_SCREENING_PATH", "")
        if not screening_src_file_path:
            exit_with_error(logger, f"screening source file path should be provided!")

        screening_delta_table_path = os.environ.get("SCREENING_DELTA_TABLE_PATH", "")
        if not screening_delta_table_path:
            exit_with_error(logger, f"screening delta table path should be provided!")

        # read screening source csv file from s3 bucket
        logger.warn(f"read screening source csv file from s3")
        src_df = spark.read.csv(screening_src_file_path, header=True)
        screening_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("documented_date", get_column(src_df, "Documented_Date", DateType())) \
            .withColumn("tobacco_use_screening_done", get_column(src_df, "Tobacco_use_screening_Done", StringType())) \
            .withColumn("tobacco_use_screening_code", get_column(src_df, "Tobacco_use_screening_code", StringType())) \
            .withColumn("tobacco_use_screening_code_system", get_column(src_df, "Tobacco_use_screening_code_system", StringType())) \
            .withColumn("smoking_status", get_column(src_df, "Smoking_Status", StringType())) \
            .withColumn("smoking_status_code", get_column(src_df, "Smoking_Status_Code", StringType())) \
            .withColumn("smoking_status_code_system", get_column(src_df, "Smoking_Status_Code_system", StringType())) \
            .withColumn("smoking_cessation", get_column(src_df, "Smoking_Cessation", StringType())) \
            .withColumn("smoking_cessation_code", get_column(src_df, "Smoking_Cessation_Code", StringType())) \
            .withColumn("smoking_cessation_code_system", get_column(src_df, "Smoking_Cessation_Code_system", StringType())) \
            .withColumn("alcohol_use_dependence", get_column(src_df, "Alcohol_use_dependence", StringType())) \
            .withColumn("phq-9_value", get_column(src_df, "PHQ-9_Value", StringType())) \
            .withColumn("mammography_screening_done", get_column(src_df, "Mammography_Screening_Done", StringType())) \
            .withColumn("mammography_screening_code", get_column(src_df, "Mammography_Screening_Code", StringType())) \
            .withColumn("mammography_screening_code_system", get_column(src_df, "Mammography_Screening_Code_System", StringType())) \
            .withColumn("ct_colonography_performed", get_column(src_df, "CT_Colonography_Performed", StringType())) \
            .withColumn("ct_colonography_performed_code", get_column(src_df, "CT_Colonography_Performed_Code", StringType())) \
            .withColumn("ct_colonography_performed_code_system", get_column(src_df, "CT_Colonography_Performed_Code_System", StringType())) \
            .withColumn("colonoscopy_done", get_column(src_df, "Colonoscopy_Done", StringType())) \
            .withColumn("colonoscopy_code", get_column(src_df, "Colonoscopy_Code", StringType())) \
            .withColumn("colonoscopy_code_system", get_column(src_df, "Colonoscopy_Code_System", StringType())) \
            .withColumn("flexible_sigmoidoscopy_done", get_column(src_df, "Flexible_Sigmoidoscopy_Done", StringType())) \
            .withColumn("flexible_sigmoidoscopy_code", get_column(src_df, "Flexible_Sigmoidoscopy_code", StringType())) \
            .withColumn("flexible_sigmoidoscopy_code_system", get_column(src_df, "Flexible_Sigmoidoscopy_code_System", StringType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .select([
                "client_name",
                "client_id",
                "health_token_id",
                "documented_date",
                "tobacco_use_screening_done",
                "tobacco_use_screening_code",
                "tobacco_use_screening_code_system",
                "smoking_status",
                "smoking_status_code",
                "smoking_status_code_system",
                "smoking_cessation",
                "smoking_cessation_code",
                "smoking_cessation_code_system",
                "alcohol_use_dependence",
                "phq-9_value",
                "mammography_screening_done",
                "mammography_screening_code",
                "mammography_screening_code_system",
                "ct_colonography_performed",
                "ct_colonography_performed_code",
                "ct_colonography_performed_code_system",
                "colonoscopy_done",
                "colonoscopy_code",
                "colonoscopy_code_system",
                "flexible_sigmoidoscopy_done",
                "flexible_sigmoidoscopy_code",
                "flexible_sigmoidoscopy_code_system",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.screening (
                client_name STRING,
                client_id INTEGER,
                health_token_id BINARY,
                documented_date DATE,
                tobacco_use_screening_done STRING,
                tobacco_use_screening_code STRING,
                tobacco_use_screening_code_system STRING,
                smoking_status STRING,
                smoking_status_code STRING,
                smoking_status_code_system STRING,
                smoking_cessation STRING,
                smoking_cessation_code STRING,
                smoking_cessation_code_system STRING,
                alcohol_use_dependence STRING,
                phq_9_value STRING,
                mammography_screening_done STRING,
                mammography_screening_code STRING,
                mammography_screening_code_system STRING,
                ct_colonography_performed STRING,
                ct_colonography_performed_code STRING,
                ct_colonography_performed_code_system STRING,
                colonoscopy_done STRING,
                colonoscopy_code STRING,
                colonoscopy_code_system STRING,
                flexible_sigmoidoscopy_done STRING,
                flexible_sigmoidoscopy_code STRING,
                flexible_sigmoidoscopy_code_system STRING,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/screening'
        """)

        # write data into delta lake screening_df table
        screening_df.write.format("delta").mode("append").save(screening_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
