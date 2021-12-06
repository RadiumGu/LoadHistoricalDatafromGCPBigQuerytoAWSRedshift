"""Spark Code to convert csv.gz -> parquet file for historical dataset."""
import logging
import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, current_date, lit, sha2
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.types import IntegerType, StringType, TimestampType

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel("INFO")


def main(argv):
    """Main program to run the Spark job."""
    source_location = argv[0]
    destination_location = argv[1]


    logger.info("********* Input parameters *********")

    logger.info("source_location :" + source_location)
    logger.info("destination_location :" + destination_location)

    logger.info("*************************************")

    sc = SparkContext()

    spark = (
        SparkSession.builder.appName("HistoryLoad-spark-job")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )

    # 512 MBs per partition
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.split.minsize", "536870912"
    )
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.split.maxsize", "536870912"
    )
    spark.conf.set("spark.kryoserializer.buffer.max", "4096m")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.rdd.compress", "true")

    temp_df = spark.read.csv(source_location, header=True)

    output_df = (
        temp_df.withColumn("AffidavitID", temp_df["AffidavitID"].cast(IntegerType()))
        .withColumn("AffidavitDetailID", temp_df["AffidavitDetailID"].cast(IntegerType()))
        .withColumn("SpotISCI", temp_df["SpotISCI"].cast(StringType()))
        .withColumn("Advertiser", temp_df["Advertiser"].cast(StringType()))
        .withColumn("adv_code", temp_df["adv_code"].cast(StringType()))
        .withColumn("agy_code", temp_df["agy_code"].cast(StringType()))
        .withColumn("SalNo", temp_df["SalNo"].cast(IntegerType()))
        .withColumn("SalRev", temp_df["SalRev"].cast(StringType()))
    )

    # Create the meta_ap_id by calcualting the hash of all columns

    output_df = (
        output_df.withColumn("meta_ap_id", sha2(concat_ws("", *output_df.columns), 256))
        .withColumn("meta_process_id", lit(source_location.split("/")[4]))
        .withColumn("meta_load_date", current_date().cast(TimestampType()))
        .withColumn("meta_update_date", current_date().cast(TimestampType()))
        .withColumn("meta_from_file", lit(source_location))
    )

    output_df.write.mode("overwrite").parquet(
        destination_location
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception("Incorrect number of arguments passed")

    print("Number of arguments:, {}, arguments.".format(len(sys.argv)))
    print("Argument List:, {}".format(str(sys.argv)))

    main(sys.argv[1:])