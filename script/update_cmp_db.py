# 更新比较服务器模型状态

import sys 

from pymongo import MongoClient
from pymongo import UpdateOne
from bson.objectid import ObjectId 


# 命令行参数 
instanceId = sys.argv[1]  # 实例ID
state = sys.argv[2]       # 运行状态
progress = sys.argv[3]    # 进度
MongoUrl = sys.argv[4]
DbName = sys.argv[5]
ColName = sys.argv[6]

def main():
    client = MongoClient(MongoUrl)
    db = client[DbName]
    col = db[ColName]

    result = col.update_one(
        { '_id': ObjectId(instanceId) },
        {
            '$set': {
                'state': state,
                'progress': progress
            }
        }
    )

if __name__ == "__main__": 
    main()