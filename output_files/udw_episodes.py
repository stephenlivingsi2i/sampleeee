import os
from pyspark.sql.types import IntegerType, DateType, BinaryType, StringType,DecimalType,LongType,FloatType, ShortType,TimestampType

from spark import (
    start_spark,
    set_s3_credentials_provider,
    exit_with_error,
    get_column,
)

os.environ['UDW_EPISODES_PATH'] = 's3a://delta-lake-tables/udw/episodes.csv'
os.environ['EPISODES_DELTA_TABLE_PATH'] = 's3a://delta-lake-tables/delta-tables/udw/episodes'

### Important: please export below environment variables
# export UDW_BENEFICIARY_DETAILS_PATH="s3a://delta-lake-tables/udw/beneficiary_details.csv"
# export BENEFICIARY_DETAILS_DELTA_TABLE_PATH="s3a://delta-lake-tables/delta-tables/udw/beneficiary-details"


def main():
    app_name = "udw_episodes_load"
    # create spark session
    spark, logger = start_spark(app_name)
    
    try:
        # add s3 credentials in hadoop configuration
        spark = set_s3_credentials_provider(spark)

        # validate environment variables
        episodes_src_file_path = os.environ.get("UDW_EPISODES_PATH", "")
        if not episodes_src_file_path:
            exit_with_error(logger, f"episodes source file path should be provided!")

        episodes_delta_table_path = os.environ.get("EPISODES_DELTA_TABLE_PATH", "")
        if not episodes_delta_table_path:
            exit_with_error(logger, f"episodes delta table path should be provided!")

        # read practice source csv file from s3 bucket
        logger.warn(f"read episodes_delta_table_path source csv file from s3")
        src_df = spark.read.csv(episodes_src_file_path, header=True)
        episodes_df = src_df \
            .withColumn("id", get_column(src_df, "ID", IntegerType())) \
            .withColumn("ben_hicn", get_column(src_df, "BEN_HICN", StringType())) \
            .withColumn("health_record_key", get_column(src_df, "HealthRecordKey", LongType())) \
            .withColumn("health_token_id", get_column(src_df, "HealthTokenID", BinaryType())) \
            .withColumn("dod", get_column(src_df, "DOD", DateType())) \
            .withColumn("ep_id", get_column(src_df, "EP_ID", LongType())) \
            .withColumn("ep_beg", get_column(src_df, "EP_BEG", DateType())) \
            .withColumn("ep_end", get_column(src_df, "EP_END", DateType())) \
            .withColumn("ep_length", get_column(src_df, "EP_LENGTH", StringType())) \
            .withColumn("cancer_type", get_column(src_df, "CANCER_TYPE", StringType())) \
            .withColumn("recon_elig", get_column(src_df, "RECON_ELIG", StringType())) \
            .withColumn("dual_ptd_lis", get_column(src_df, "DUAL_PTD_LIS", StringType())) \
            .withColumn("inst", get_column(src_df, "INST", StringType())) \
            .withColumn("radiation", get_column(src_df, "RADIATION", StringType())) \
            .withColumn("hcc_grp", get_column(src_df, "HCC_GRP", StringType())) \
            .withColumn("hrr_rel_cost", get_column(src_df, "HRR_REL_COST", DecimalType())) \
            .withColumn("surgery", get_column(src_df, "SURGERY", StringType())) \
            .withColumn("clinical_trial", get_column(src_df, "CLINICAL_TRIAL", StringType())) \
            .withColumn("bmt", get_column(src_df, "BMT", StringType())) \
            .withColumn("clean_pd", get_column(src_df, "CLEAN_PD", StringType())) \
            .withColumn("ptd_chemo", get_column(src_df, "PTD_CHEMO", StringType())) \
            .withColumn("actual_exp", get_column(src_df, "ACTUAL_EXP", DecimalType())) \
            .withColumn("baseline_price", get_column(src_df, "BASELINE_PRICE", DecimalType())) \
            .withColumn("experience_adj", get_column(src_df, "EXPERIENCE_ADJ", DecimalType())) \
            .withColumn("actual_exp_unadj", get_column(src_df, "ACTUAL_EXP_UNADJ", StringType())) \
            .withColumn("cast_sens_pros", get_column(src_df, "CAST_SENS_PROS", StringType())) \
            .withColumn("low_risk_blad", get_column(src_df, "LOW_RISK_BLAD", StringType())) \
            .withColumn("client_id", get_column(src_df, "ClientID", IntegerType())) \
            .withColumn("client_name", get_column(src_df, "ClientName", StringType())) \
            .withColumn("target_created_date", get_column(src_df, "TargetCreatedDate", TimestampType())) \
            .withColumn("hash_bytes_sha", get_column(src_df, "HashBytesSHA", BinaryType())) \
            .select([
                "id",
                "ben_hicn",
                "health_record_key",
                "health_token_id",
                "dod",
                "ep_id",
                "ep_beg",
                "ep_end",
                "ep_length",
                "cancer_type",
                "recon_elig",
                "dual_ptd_lis",
                "inst",
                "radiation",
                "hcc_grp",
                "hrr_rel_cost",
                "surgery",
                "clinical_trial",
                "bmt",
                "clean_pd",
                "ptd_chemo",
                "actual_exp",
                "baseline_price",
                "experience_adj",
                "actual_exp_unadj",
                "cast_sens_pros",
                "low_risk_blad",
                "client_id",
                "client_name",
                "target_created_date",
                "hash_bytes_sha"
            ])
        spark.sql("CREATE DATABASE IF NOT EXISTS udw")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS udw.episodes (
                id INTEGER,
                ben_hicn STRING,
                health_record_key LONG,
                health_token_id BINARY,
                dod DATE,
                ep_id LONG,
                ep_beg DATE,
                ep_end DATE,
                ep_length STRING,
                cancer_type STRING,
                recon_elig STRING,
                dual_ptd_lis STRING,
                inst STRING,
                radiation STRING,
                hcc_grp STRING,
                hrr_rel_cost DECIMAL,
                surgery STRING,
                clinical_trial STRING,
                bmt STRING,
                clean_pd STRING,
                ptd_chemo STRING,
                actual_exp DECIMAL,
                baseline_price DECIMAL,
                experience_adj DECIMAL,
                actual_exp_unadj STRING,
                cast_sens_pros STRING,
                low_risk_blad STRING,
                client_id INTEGER,
                client_name STRING,
                target_created_date TIMESTAMP,
                hash_bytes_sha BINARY
            )
            USING delta
            PARTITIONED BY (client_id)
            LOCATION 's3a://delta-lake-tables/delta-tables/udw/episodes'
        """)

        # write data into delta lake episodes table
        episodes_df.write.format("delta").mode("append").save(episodes_delta_table_path)

        logger.warn(
            f"spark job {app_name} completed successfully."
        )


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
