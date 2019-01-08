# -*- coding: UTF-8 -*-
"""
spark操作hadoop准备数据
生成样本sequence file
"""
import sys
from pyspark.sql import SparkSession

# 命令行参数
APPNAME = sys.argv[1]
HADOOP_URL = 'hdfs://parallel.master.weave.local:8020'  # hdfs://172.21.212.122:9000
INPUT_DIR = '/model/IBIS/param'   # /site/ibis/file/sites
OUTPUT_PATH = '/model/IBIS/seqFile/param_100'  # /site/ibis/seqFile/10000

def getTargetSites(indexList):
    return

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName(APPNAME)\
        .getOrCreate()

    sc = spark.sparkContext

    inputDir = HADOOP_URL + INPUT_DIR
    outputDir = HADOOP_URL + OUTPUT_PATH

    rdd = sc.wholeTextFiles(inputDir)
    rdd.saveAsSequenceFile(outputDir)

    m = rdd.collect()

    print(m)

    spark.stop()