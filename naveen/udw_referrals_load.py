import os
from pyspark.sql.types import IntegerType, DateType, BinaryType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

### Important: please export below environment variables
# export UDW_REFERRALS_FILE_PATH="s3a://sureshn-sandbox-landing-bucket/udw/referrals.csv"
# export REFERRAL_DELTA_TABLE_PATH="s3a://sureshn-sandbox-landing-bucket/delta-tables/udw/referrals"


def main():
    app_name = "udw_referrals_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        referrals_src_file_path = os.environ.get("UDW_REFERRALS_FILE_PATH", "")
        if not referrals_src_file_path:
            exit_with_error(logger, f"referrals source file path should be provided!")

        referrals_delta_table_path = os.environ.get("REFERRALS_DELTA_TABLE_PATH", "")
        if not referrals_delta_table_path:
            exit_with_error(logger, f"referrals delta table path should be provided!")

        # read referrals source csv file from s3 bucket
        logger.warn(f"read referrals source csv file from s3")
        src_df = spark.read.csv(referrals_src_file_path, header=True)
        
        referrals_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName")) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("provider_first_name", get_column(src_df, "PROVIDER_FIRST_NAME")) \
            .withColumn("provider_last_name", get_column(src_df, "PROVIDER_LAST_NAME")) \
            .withColumn("npi", get_column(src_df, "NPI")) \
            .withColumn("referral_date", get_column(src_df, "REFERRAL_DATE", DateType())) \
            .withColumn("speciality", get_column(src_df, "SPECIALITY")) \
            .withColumn("primary_diagnosis_code", get_column(src_df, "Primary_Diagnosis_Code")) \
            .withColumn("referral_status", get_column(src_df, "REFERRAL_STATUS")) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY")) \
            .withColumn("created_date", get_column(src_df, "CREATED_DATE", TimestampType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY")) \
            .withColumn("updated_date", get_column(src_df, "UPDATED_DATE", TimestampType())) \
            .withColumn("reason_for_referral", get_column(src_df, "REASON_FOR_REFERRAL")) \
            .select([
                "client_name", 
                "client_id",
                "health_token_id",
                "provider_first_name",
                "provider_last_name",
                "npi",
                "referral_date",
                "speciality",
                "primary_diagnosis_code",
                "referral_status",
                "created_by",
                "created_date",
                "updated_by",
                "updated_date",
                "reason_for_referral",
            ])

        # create database `udw` if not exists in the delta lake
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")

        # create table if not exists in the delta lake metastore
        spark.sql("""
        CREATE TABLE IF NOT EXISTS udw.referrals(
            client_name STRING,
            client_id INTEGER,
            health_token_id BINARY,
            provider_first_name STRING,
            provider_last_name STRING,
            npi STRING,
            referral_date DATE,
            speciality STRING,
            primary_diagnosis_code STRING,
            referral_status STRING,
            created_by STRING,
            created_date TIMESTAMP,
            updated_by STRING,
            updated_date TIMESTAMP,
            reason_for_referral STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION ''
        """)

        # write data into delta lake practice table
        referrals_df.write.format("delta").mode("append").save(referrals_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()