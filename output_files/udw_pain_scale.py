import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_PAIN_SCALE_PATH'] = 's3a://delta-lake-tables/udw/pain_scale.csv'
os.environ['PAIN_SCALE_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/pain_scale'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_pain_scale_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        pain_scale_src_file_path = os.environ.get("UDW_PAIN_SCALE_PATH", "")
        if not pain_scale_src_file_path:
            exit_with_error(logger, f"pain_scale source file path should be provided!")

        pain_scale_delta_table_path = os.environ.get("PAIN_SCALE_DELTA_TABLE_PATH", "")
        if not pain_scale_delta_table_path:
            exit_with_error(logger, f"pain_scale delta table path should be provided!")

        # read pain_scale source csv file from s3 bucket
        logger.warn(f"read pain_scale source csv file from s3")
        src_df = spark.read.csv(pain_scale_src_file_path, header=True)
        pain_scale_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("pain_scale", get_column(src_df, "Pain_Scale", StringType())) \
            .withColumn("followup_for_pain", get_column(src_df, "FOLLOWUP_FOR_PAIN", StringType())) \
            .withColumn("plan_of_care_documented_for_pain", get_column(src_df, "PLAN_OF_CARE_DOCUMENTED_FOR_PAIN", StringType())) \
            .withColumn("plan_of_care_documented_for_pain_date", get_column(src_df, "PLAN_OF_CARE_DOCUMENTED_FOR_PAIN_DATE", StringType())) \
            .withColumn("assessment_name", get_column(src_df, "ASSESSMENT_NAME", StringType())) \
            .withColumn("pain_documented_date", get_column(src_df, "PAIN_Documented_Date", DateType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", StringType())) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .select([
                "client_name",
                "client_id",
                "health_token_id",
                "pain_scale",
                "followup_for_pain",
                "plan_of_care_documented_for_pain",
                "plan_of_care_documented_for_pain_date",
                "assessment_name",
                "pain_documented_date",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.pain_scale (
                client_name STRING,
                client_id INTEGER,
                health_token_id BINARY,
                pain_scale STRING,
                followup_for_pain STRING,
                plan_of_care_documented_for_pain STRING,
                plan_of_care_documented_for_pain_date STRING,
                assessment_name STRING,
                pain_documented_date DATE,
                created_by STRING,
                created_date TIMESTAMP,
                updated_by STRING,
                updated_date TIMESTAMP  
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/pain_scale'
        """)

        # write data into delta lake pain_scale table
        pain_scale_df.write.format("delta").mode("append").save(pain_scale_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
