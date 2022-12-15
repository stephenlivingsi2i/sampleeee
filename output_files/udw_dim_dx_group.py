import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_DIM_DX_GROUP_PATH'] = 's3a://delta-lake-tables/udw/dim_dx_group.csv'
os.environ['DIM_DX_GROUP_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/dim_dx_group'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_dim_dx_group_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        dim_dx_group_src_file_path = os.environ.get("UDW_DIM_DX_GROUP_PATH", "")
        if not dim_dx_group_src_file_path:
            exit_with_error(logger, f"dim_dx_group source file path should be provided!")

        dim_dx_group_delta_table_path = os.environ.get("DIM_DX_GROUP_DELTA_TABLE_PATH", "")
        if not dim_dx_group_delta_table_path:
            exit_with_error(logger, f"dim_dx_group delta table path should be provided!")

        # read dim_dx_group source csv file from s3 bucket
        logger.warn(f"read dim_dx_group source csv file from s3")
        src_df = spark.read.csv(dim_dx_group_src_file_path, header=True)
        dim_dx_group_df = src_df \
            .withColumn("dx_group_id", get_column(src_df, "Dx_Group_ID", IntegerType())) \
            .withColumn("dx_group_desc", get_column(src_df, "Dx_Group_Desc", StringType())) \
            .select([
                "dx_group_id",
                "dx_group_desc",
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.dim_dx_group (
                dx_group_id INTEGER,
                dx_group_desc STRING
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/dim_dx_group'
        """)

        # write data into delta lake dim_dx_group table
        dim_dx_group_df.write.format("delta").mode("append").save(dim_dx_group_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
