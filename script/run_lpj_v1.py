import os, errno
import subprocess
import sys
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import Row

def fakeRunModel(site):
    # project base path
    basePath = '/home/bowen/LPJ'

    instanceId = site[1]
    siteId = str(site[0])

    outputPath = basePath + '/instance/' + instanceId
    mockPath = basePath + '/instance/output_mock'

    # output elements
    eleList = ['annual_lai', 'annual_litter_ag', 'annual_rh', 'fpc']

    for i in eleList:
        # if folder exist
        eleFolderPath = outputPath + '/' + i
        isExists = os.path.exists(eleFolderPath)
        if not isExists:
            try:
                os.makedirs(eleFolderPath)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        
        # copy output file
        fileName = siteId + '_' + i + '.ascii'
        srcFile = mockPath + '/' + i + '/' + fileName
        desFile = eleFolderPath + '/' + fileName
        shutil.copyfile(srcFile, desFile)
        # stop thread as model running
        time.sleep(10)

    return site

def main():
    # spark init
    spark = SparkSession\
        .builder\
        .getOrCreate()

    # command arguments 
    instanceId = sys.argv[1]
    startId = int(sys.argv[2])
    endId = int(sys.argv[3])

    # get site index list
    siteList = []
    for i in range(startId, endId + 1):
        siteList.append([i, instanceId])

    # spark calculate
    rdd = spark.sparkContext.parallelize(siteList, len(siteList))
    map_rdd = rdd.map(fakeRunModel)
    map_rdd.count()

    spark.stop()

    return

if __name__ == "__main__":
    main()