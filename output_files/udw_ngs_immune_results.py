import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_NGS_IMMUNE_RESULTS_PATH'] = 's3a://delta-lake-tables/udw/ngs_immune_results.csv'
os.environ['NGS_IMMUNE_RESULTS_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/ngs_immune_results'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_ngs_immune_results_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        ngs_immune_results_src_file_path = os.environ.get("UDW_NGS_IMMUNE_RESULTS_PATH", "")
        if not ngs_immune_results_src_file_path:
            exit_with_error(logger, f"ngs_immune_results source file path should be provided!")

        ngs_immune_results_delta_table_path = os.environ.get("NGS_IMMUNE_RESULTS_DELTA_TABLE_PATH", "")
        if not ngs_immune_results_delta_table_path:
            exit_with_error(logger, f"ngs_immune_results delta table path should be provided!")

        # read ngs_immune_results source csv file from s3 bucket
        logger.warn(f"read ngs_immune_results source csv file from s3")
        src_df = spark.read.csv(ngs_immune_results_src_file_path, header=True)
        ngs_immune_results_df = src_df \
            .withColumn("immune_id", get_column(src_df, "Immune_id", IntegerType())) \
            .withColumn("order_id", get_column(src_df, "Order_id", StringType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", IntegerType())) \
            .withColumn("name", get_column(src_df, "name", StringType())) \
            .withColumn("go_name", get_column(src_df, "go_name", StringType())) \
            .withColumn("test", get_column(src_df, "test", StringType())) \
            .withColumn("marker", get_column(src_df, "marker", StringType())) \
            .withColumn("gene_description", get_column(src_df, "gene_description", StringType())) \
            .withColumn("result_numeric", get_column(src_df, "result_numeric", StringType())) \
            .withColumn("result_string", get_column(src_df, "result_string", StringType())) \
            .withColumn("units", get_column(src_df, "units", StringType())) \
            .withColumn("interpretation", get_column(src_df, "interpretation", StringType())) \
            .withColumn("immune_cycle_role", get_column(src_df, "immune_cycle_role", StringType())) \
            .withColumn("marker_profiles_name", get_column(src_df, "marker_profiles_name", StringType())) \
            .withColumn("marker_profiles_components", get_column(src_df, "marker_profiles_components", StringType())) \
            .withColumn("immune_phenotype", get_column(src_df, "immune_phenotype", StringType())) \
            .withColumn("therapy_considerations_level_of_evidence", get_column(src_df, "therapy_considerations_level_of_evidence", StringType())) \
            .withColumn("therapy_considerations_parenthetical_statements", get_column(src_df, "therapy_considerations_parenthetical_statements", StringType())) \
            .withColumn("references_title", get_column(src_df, "references_title", StringType())) \
            .withColumn("clinical_trials_distance_description", get_column(src_df, "clinical_trials_distance_description", StringType())) \
            .withColumn("references_index", get_column(src_df, "references_index", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .select([
                "immune_id",
                "order_id",
                "health_record_key",
                "name",
                "go_name",
                "test",
                "marker",
                "gene_description",
                "result_numeric",
                "result_string",
                "units",
                "interpretation",
                "immune_cycle_role",
                "marker_profiles_name",
                "marker_profiles_components",
                "immune_phenotype",
                "therapy_considerations_level_of_evidence",
                "therapy_considerations_parenthetical_statements",
                "references_title",
                "clinical_trials_distance_description",
                "references_index",
                "client_id",
                "client_name",
                "health_token_id"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.ngs_immune_results (
                immune_id INTEGER,
                order_id STRING,
                health_record_key INTEGER,
                name STRING,
                go_name STRING,
                test STRING,
                marker STRING,
                gene_description STRING,
                result_numeric STRING,
                result_string STRING,
                units STRING,
                interpretation STRING,
                immune_cycle_role STRING,
                marker_profiles_name STRING,
                marker_profiles_components STRING,
                immune_phenotype STRING,
                therapy_considerations_level_of_evidence STRING,
                therapy_considerations_parenthetical_statements STRING,
                references_title STRING,
                clinical_trials_distance_description STRING,
                references_index STRING,
                client_id INTEGER,
                client_name STRING,
                health_token_id BINARY
        )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/ngs_immune_results'
        """)

        # write data into delta lake ngs_immune_results table
        ngs_immune_results_df.write.format("delta").mode("append").save(ngs_immune_results_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
