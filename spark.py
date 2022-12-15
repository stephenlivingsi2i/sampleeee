import sys
from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import DataType, StringType

import spark_logger


def start_spark(app_name="hec_data_pipeline_job"):
    """Start spark session and also configure logger format with app details.

    :param  app_name: Name of Spark app
    :return: A tuple of spark session and logger references
    """
    spark = SparkSession.builder.appName(app_name).config("spark.jars.packages", \
         "io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.2.2").getOrCreate()
    logger = spark_logger.Log4j(spark)
    return spark, logger


def set_s3_credentials_provider(spark: SparkSession) -> SparkSession:
    # set AWS simple credentials provider in spark context
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key",
        "AKIAYRRDYOCO5DYHSVLN",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key",
        "PXosT7XXiHWDkoQmTOFNDmnE4C924/+RmKcWqlCV",
    )
    # # set AWS web identity token credentials provider in spark context
    # spark.sparkContext._jsc.hadoopConfiguration().set(
    #     "fs.s3a.aws.credentials.provider",
    #     "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    # )
    # set AWS S3 configuration in spark context
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.connection.ssl.enabled",
        "true",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint",
        "s3.amazonaws.com",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.fast.upload",
        "true",
    )
    return spark


def exit_with_error(log, message):
    log.error(message)
    sys.exit(1)


def get_column(df: DataFrame, column: str, dtype: DataType = StringType()):
    try:
        return df[column]
    except AnalysisException:
        return f.lit(None).cast(dtype)
