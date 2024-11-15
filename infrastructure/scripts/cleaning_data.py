"""
модуль для очистки данных
Для запуска в качестве параметра подается
название файла на hdfs

Результат:
очищенный файл в формате parquet

Пример запуска c мастер ноды:

python3 cleaning_data.py -fn '2022-11-04.txt'

"""
import findspark

findspark.init()

from loguru import logger
import argparse
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import IntegerType,LongType,DoubleType,StringType

data_path = "/user/ubuntu/data/"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file_name",
        "-fn",
        help="name of file on hdfs",
        required=True,
        type=str,
        dest="file_name",
    )

    args = parser.parse_args()
    return args

# генерация названия файлика-лога
def get_spark():

    spark = (
            SparkSession
            .builder
            .appName("OTUS")
            .getOrCreate()
            )
    return spark

def main():

    logger.info("Spark session has been estableshed")

    findspark.init()
    logger.info("Spark init - done")

    spark = get_spark()
    logger.info("Spark session has been estableshed")

    args = parse_args()

    path_file = f'{data_path}{args.file_name}'
    short_name = args.file_name.split('.')[0].replace('-','')

    logger.info(f"full file path: {path_file}. Part column is {short_name}")

    sdf = spark.read.text(path_file)

    split_col = F.split(sdf['value'], ',')

    sdf_split = (sdf
                .withColumn('tranaction_id', split_col.getItem(0))
                .withColumn('tx_datetime', split_col.getItem(1))
                .withColumn('customer_id', split_col.getItem(2))
                .withColumn('terminal_id', split_col.getItem(3))
                .withColumn('tx_amount', split_col.getItem(4))
                .withColumn('tx_time_seconds', split_col.getItem(5))
                .withColumn('tx_time_days', split_col.getItem(6))
                .withColumn('tx_fraud', split_col.getItem(7))
                .withColumn('tx_fraud_scenario', split_col.getItem(8))
                .drop(*['value'])
                .filter(~F.lower(F.col('tranaction_id')).like('%tranaction_id%'))
               )

    sdf_agg = (
                sdf_split
                .select(*['tx_amount'])
                .dropna()
                .agg(
                    F.round(F.expr("percentile(tx_amount, array(0.01))")[0], 4).alias("tx_amount_1perc"),
                    F.round(F.expr("percentile(tx_amount, array(0.99))")[0], 4).alias("tx_amount_99perc")
                    )
               )

    col_list = [
                'tranaction_id',
                'tx_datetime',
                'customer_id',
                'terminal_id',
                'tx_amount',
                'tx_time_seconds',
                'tx_time_days',
                'tx_fraud',
                'tx_fraud_scenario'
                ]

    sdf_clean = (
                sdf_split
                .join(sdf_agg, how='left')
                .filter(F.col('tx_amount')>0)
                .filter(F.col('tx_amount')>=F.col('tx_amount_1perc'))
                .filter(F.col('tx_amount')<=F.col('tx_amount_99perc'))
                .dropna()
                .filter(~F.col('terminal_id').rlike('[a-zA-Z]')) # только альфа-нумерик
                .filter(~F.col('customer_id').rlike('[a-zA-Z]')) # только альфа-нумерик
                .filter(~F.col('tranaction_id').rlike('[a-zA-Z]')) # только альфа-нумерик
                .filter(~F.col('tx_fraud').rlike('[a-zA-Z]')) # только альфа-нумерик
                .filter(~F.col('tx_fraud_scenario').rlike('[a-zA-Z]')) # только альфа-нумерик
                .filter(F.col('tx_time_seconds')>0) # только альфа-нумерик
                .filter(F.col('tx_time_days')>0) # только альфа-нумерик
                .select(*col_list)
                .distinct()
                .withColumn('tranaction_id', F.col('tranaction_id').cast(StringType()))
                .withColumn('tx_datetime', F.col('tx_datetime').cast(StringType()))
                .withColumn('customer_id', F.col('customer_id').cast(StringType()))
                .withColumn('terminal_id', F.col('terminal_id').cast(StringType()))
                .withColumn('tx_amount', F.col('tx_amount').cast(DoubleType()))
                .withColumn('tx_time_seconds', F.col('tx_time_seconds').cast(LongType()))
                .withColumn('tx_time_days', F.col('tx_time_days').cast(LongType()))
                .withColumn('tx_fraud', F.col('tx_fraud').cast(IntegerType()))
                .withColumn('tx_fraud_scenario', F.col('tx_fraud_scenario').cast(IntegerType()))
                .withColumn('part_date', F.lit(short_name))
                )

    output_hdfs_table_path = f'{data_path}{short_name}.parquet'

    (
    sdf_clean
    .repartition(10)
    .write
    #.partitionBy(*partition_cols)
    .mode("overwrite")
    .parquet(output_hdfs_table_path)
    #.parquet("data/train.parquet")
    )

    logger.info(f"data has been saved to : {output_hdfs_table_path} succesfully")
    logger.info("job is done")

if __name__ == '__main__':
    main()
