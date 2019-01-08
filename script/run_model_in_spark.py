# -*- coding: UTF-8 -*-
"""
调用模型并行计算
输入使用普通hadoop file

1 从Hadoop提取数据并转换为输入数据
2 通过Wine调用window模型
3 结果数据保存至Hadoop
"""

import sys
import os
import subprocess
import shutil
import datetime
from pyspark.sql import SparkSession

# 命令行参数
INSTANCE_ID = sys.argv[1]
CALC_SITE = sys.argv[2]
calcList = CALC_SITE.split(',')
# calcList = []
# for i in range(1, 101):
#     calcList.append(str(i))

HADOOP_URL = 'hdfs://parallel.master.weave.local:8020'  # hdfs://10.36.0.2:9000
HADOOP_INPUT_DIR =  HADOOP_URL + '/model/IBIS'
HADOOP_OUTPUT_DIR = HADOOP_URL + '/model/IBIS/instance/' + INSTANCE_ID
# OUTPUT_DIR = '/home/bowen/Parallel/instance'
MODEL_PATH = '/opt/model/IBIS/IBIS.exe'
OUTPUT_DIR = '/opt/model/instance/' + INSTANCE_ID


# 运算的站点列表
# calcList = ['1', '2', '3','4', '5', '6', '7', '8', '9', '10']

# 返回实际需要计算的站点RDD
def filterCalcParamList(hdfsObj):
    # 获取站点索引
    hdfsIndex = hdfsObj[0]    
    hdfsList = hdfsIndex.split('/')
    siteIndex = hdfsList[len(hdfsList) - 1].split('.')[0]

    isCalcIndex = calcList.count(siteIndex)
    if isCalcIndex == 0:
        return False
    else:
        return True

def filterCalcSiteList(hdfsObj):
    # 获取站点索引
    hdfsIndex = hdfsObj[0]    
    hdfsList = hdfsIndex.split('/')
    siteIndex = hdfsList[len(hdfsList) - 1].split('_')[0]

    isCalcIndex = calcList.count(siteIndex)
    if isCalcIndex == 0:
        return False
    else:
        return True        

def siteIndexStandized(Obj):
    hdfsIndex = Obj[0]    
    hdfsList = hdfsIndex.split('/')
    index = hdfsList[len(hdfsList) - 1].split('_')[0]

    return [index, Obj[1]]

def paramIndexStandized(Obj):
    hdfsIndex = Obj[0]    
    hdfsList = hdfsIndex.split('/')
    index = hdfsList[len(hdfsList) - 1].split('.')[0]
    return [index, Obj[1]]   

# 计算
def calc(Obj):
    # rdd2file
    siteContent = Obj[1][0]
    paramContent = Obj[1][1]

    baseDir = OUTPUT_DIR + '/' + Obj[0]
    os.makedirs(baseDir, 777, True)

    sitePath = baseDir + '/site.csv'
    with open(sitePath, 'w') as f:
        f.write(siteContent)

    paramPath = baseDir + '/param.txt'
    with open(paramPath, 'w') as f:
        f.write(paramContent)

    # invoke model
    index = Obj[0]
    outputPath = OUTPUT_DIR + '/' + index + '/output.txt'

    devNull = open(os.devnull, 'w')
    subprocess.call('wine ' + MODEL_PATH + ' -i=' + sitePath + ' -o=' + outputPath + ' -s=' + paramPath, shell=True, stdout=None)

    # 结果数据转化为RDD
    outputContent = ''
    with open(outputPath, 'r') as f:
        outputContent = f.read()

    return (Obj[0], outputContent)

# 清除中间缓存
def clearCache(Obj):
    baseDir = OUTPUT_DIR + '/' + Obj[0]
    shutil.rmtree(baseDir)
    return

def run(rdd):
    index = rdd

    # IBIS Model Input
    siteHDFSPath = HADOOP_INPUT_DIR + '/site/' + index + '_proced.csv'
    paramHDFSPath = HADOOP_INPUT_DIR + '/param/' + index + '.txt'

    outputDir = OUTPUT_DIR + '/' + index
    os.makedirs(outputDir, 777, True)

    # To Local File
    devNull = open(os.devnull, 'w')
    sitePath = outputDir + '/site.csv'
    paramPath = outputDir + '/param.txt'

    subprocess.call('hadoop fs -get ' + siteHDFSPath + ' ' + sitePath, shell=True, stdout=devNull)
    subprocess.call('hadoop fs -get ' + paramHDFSPath + ' ' + paramPath, shell=True, stdout=devNull)

    outputPath = outputDir + '/output.txt'

    # run model
    subprocess.call('wine ' + MODEL_PATH + ' -i=' + sitePath + ' -s=' + paramPath + ' -o=' + outputPath, shell=True, stdout=devNull)

    # upload result
    outputHDFSPath = HADOOP_OUTPUT_DIR + '/output_' + index + '.txt'
    subprocess.call('hadoop fs -mkdir -p ' + outputHDFSPath, shell=True, stdout=devNull)
    subprocess.call('hadoop fs -put ' + outputPath + ' ' + outputHDFSPath, shell=True, stdout=devNull)

    # clear temp file
    shutil.rmtree(outputDir)

    return index

if __name__ == "__main__":
    starttime = datetime.datetime.now()

    spark = SparkSession\
        .builder\
        .appName(INSTANCE_ID)\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print('\n\nstart calcutlate----------\n')
    print(starttime)

    # 分割数据
    rddInput = sc.parallelize(calcList, len(calcList))
    #calculate
    rddOutput = rddInput.map(run)

    success_site = rddOutput.collect()
    endtime = datetime.datetime.now()

    print('\ncalculate site index:')
    print(success_site)
    print('\ncalculate complete------------- \n')
    print(endtime)

    spark.stop()