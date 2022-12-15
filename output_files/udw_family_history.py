import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_FAMILY_HISTORY_PATH'] = 's3a://delta-lake-tables/udw/family_history.csv'
os.environ['FAMILY_HISTORY_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/family_history'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_family_history_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        family_history_src_file_path = os.environ.get("UDW_FAMILY_HISTORY_PATH", "")
        if not family_history_src_file_path:
            exit_with_error(logger, f"family_history source file path should be provided!")

        family_history_delta_table_path = os.environ.get("FAMILY_HISTORY_DELTA_TABLE_PATH", "")
        if not family_history_delta_table_path:
            exit_with_error(logger, f"family_history delta table path should be provided!")

        # read family_history source csv file from s3 bucket
        logger.warn(f"read family_history source csv file from s3")
        src_df = spark.read.csv(family_history_src_file_path, header=True)
        family_history_df = src_df \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("relationship", get_column(src_df, "RELATIONSHIP", StringType())) \
            .withColumn("genetic", get_column(src_df, "Genetic", StringType())) \
            .withColumn("gender", get_column(src_df, "Gender", StringType())) \
            .withColumn("full_name_of_the_relation", get_column(src_df, "Full_Name_of_the_relation", StringType())) \
            .withColumn("dob_of_the_relation", get_column(src_df, "DOB_of_the_Relation", DateType())) \
            .withColumn("age_of_the_relation", get_column(src_df, "AGE_of_the_Relation", StringType())) \
            .withColumn("status", get_column(src_df, "Status", StringType())) \
            .withColumn("cancer_history_indicator", get_column(src_df, "Cancer_History_Indicator", StringType())) \
            .withColumn("cancer_condtion_code", get_column(src_df, "Cancer_Condtion_Code", StringType())) \
            .withColumn("created_by", get_column(src_df, "CREATED_BY", StringType())) \
            .withColumn("updated_by", get_column(src_df, "UPDATED_BY", IntegerType())) \
            .withColumn("creation_date", get_column(src_df, "CREATION_DATE", TimestampType())) \
            .withColumn("updation_date", get_column(src_df, "UPDATION_DATE", TimestampType())) \
            .select([
                "client_name",
                "client_id",
                "health_token_id",
                "relationship",
                "genetic",
                "gender",
                "full_name_of_the_relation",
                "dob_of_the_relation",
                "age_of_the_relation",
                "status",
                "cancer_history_indicator",
                "cancer_condtion_code",
                "created_by",
                "updated_by",
                "creation_date",
                "updation_date"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.family_history (
                client_name STRING,
                client_id INTEGER,
                health_token_id BINARY,
                relationship STRING,
                genetic STRING,
                gender STRING,
                full_name_of_the_relation STRING,
                dob_of_the_relation DATE,
                age_of_the_relation STRING,
                status STRING,
                cancer_history_indicator STRING,
                cancer_condtion_code STRING,
                created_by STRING,
                updated_by INTEGER,
                creation_date TIMESTAMP,
                updation_date TIMESTAMP
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/family_history'
        """)

        # write data into delta lake family_history table
        family_history_df.write.format("delta").mode("append").save(family_history_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
