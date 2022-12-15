import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_FACILITY_PATH'] = 's3a://delta-lake-tables/udw/facility.csv'
os.environ['FACILITY_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/facility'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_facility_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        facility_src_file_path = os.environ.get("UDW_FACILITY_PATH", "")
        if not facility_src_file_path:
            exit_with_error(logger, f"facility source file path should be provided!")

        facility_delta_table_path = os.environ.get("FACILITY_DELTA_TABLE_PATH", "")
        if not facility_delta_table_path:
            exit_with_error(logger, f"facility delta table path should be provided!")

        # read facility source csv file from s3 bucket
        logger.warn(f"read facility source csv file from s3")
        src_df = spark.read.csv(facility_src_file_path, header=True)
        facility_df = src_df \
            .withColumn("target_facility_id", get_column(src_df, "TargetFacilityID", LongType())) \
            .withColumn("source_facility_id", get_column(src_df, "SourceFacilityID", IntegerType())) \
            .withColumn("facility_name", get_column(src_df, "FacilityName", StringType())) \
            .withColumn("facility_address", get_column(src_df, "FacilityAddress", StringType())) \
            .withColumn("facility_city", get_column(src_df, "FacilityCity", StringType())) \
            .withColumn("facility_state_name", get_column(src_df, "FacilityStateName", StringType())) \
            .withColumn("facility_zip", get_column(src_df, "FacilityZip", StringType())) \
            .withColumn("facility_contact", get_column(src_df, "FacilityContact", StringType())) \
            .withColumn("group_npi", get_column(src_df, "GroupNPI", StringType())) \
            .withColumn("facility_type", get_column(src_df, "FacilityType", IntegerType())) \
            .withColumn("practice_name", get_column(src_df, "PracticeName", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .withColumn("facility_derived_from", get_column(src_df, "FacilityDerivedFrom", StringType())) \
            .select([
                "target_facility_id",
                "source_facility_id",
                "facility_name",
                "facility_address",
                "facility_city",
                "facility_state_name",
                "facility_zip",
                "facility_contact",
                "group_npi",
                "facility_type",
                "practice_name",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha",
                "facility_derived_from"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.facility (
                target_facility_id LONG,
                source_facility_id INTEGER,
                facility_name STRING,
                facility_address STRING,
                facility_city STRING,
                facility_state_name STRING,
                facility_zip STRING,
                facility_contact STRING,
                group_npi STRING,
                facility_type INTEGER,
                practice_name STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY,
                facility_derived_from STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/facility'
        """)

        # write data into delta lake facility table
        facility_df.write.format("delta").mode("append").save(facility_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
