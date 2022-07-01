## 1. 概述
本章主要介绍如何通过pyspark读取clickhouse并写入clickhouse分布式表。

## 2. 环境准备

- 为适应线上集群环境，可保持spark版本相同

![image.png](https://cdn.nlark.com/yuque/0/2022/png/140520/1656654918566-740d072b-a329-4af1-9c8f-dc6988a4b9a6.png#clientId=u465b5286-7df6-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=264&id=u18dcc1cf&margin=%5Bobject%20Object%5D&name=image.png&originHeight=264&originWidth=717&originalType=binary&ratio=1&rotation=0&showTitle=false&size=44129&status=done&style=none&taskId=uf6166170-c896-4cb8-a552-ba664c6cc72&title=&width=717)

然后去spark官网下载spark（因为我们cdh spark版本是2.4.0）就下载对应的版本并解压到指定位置，然后设置SPARK_HOME

![image.png](https://cdn.nlark.com/yuque/0/2022/png/140520/1656655234686-606bb804-f6c4-44b8-b932-2a48833cd28b.png#clientId=u465b5286-7df6-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=748&id=u248482d9&margin=%5Bobject%20Object%5D&name=image.png&originHeight=748&originWidth=952&originalType=binary&ratio=1&rotation=0&showTitle=false&size=187905&status=done&style=none&taskId=u9c436913-0959-413b-a478-d0bb3a72de4&title=&width=952)

- clickhouse jdbc 驱动包下载

[pyspark read clickhouse by jdbc](https://www.yuque.com/imxintian/learnlib/wfzl7i?view=doc_embed)

- 配置pycharm（图例展示为spark 2.4.2版本）

加载本地spark环境
有三种方法 如图 任选其一即可
![image.png](https://cdn.nlark.com/yuque/0/2020/png/140520/1606721171545-45a3b9e6-e262-4afe-b66e-59349a327079.png#crop=0&crop=0&crop=1&crop=1&height=674&id=okplI&margin=%5Bobject%20Object%5D&name=image.png&originHeight=674&originWidth=1706&originalType=binary&ratio=1&rotation=0&showTitle=false&size=109762&status=done&style=none&title=&width=1706)
第三种方法，亲自在环境变量里设置
![image.png](https://cdn.nlark.com/yuque/0/2020/png/140520/1606721289635-eda4d85c-4172-43c2-bd37-8a1e735e7f32.png#crop=0&crop=0&crop=1&crop=1&height=1120&id=s5X8I&margin=%5Bobject%20Object%5D&name=image.png&originHeight=1120&originWidth=1480&originalType=binary&ratio=1&rotation=0&showTitle=false&size=141082&status=done&style=none&title=&width=1480)

 配置 **pyspark和py4j**

![image.png](https://cdn.nlark.com/yuque/0/2020/png/140520/1606721535115-3e98f33d-0f00-4184-bae1-b051971153b8.png#crop=0&crop=0&crop=1&crop=1&height=1276&id=aMPhy&margin=%5Bobject%20Object%5D&name=image.png&originHeight=1276&originWidth=2022&originalType=binary&ratio=1&rotation=0&showTitle=false&size=153396&status=done&style=none&title=&width=2022)

## 3. 建表

```plsql
create TABLE data_report.dwd_test_replica on cluster hgj_clickhouse_2shards_2replicas
(
  `customer_id` String COMMENT '企业 ID',
  
  `company_name` Nullable(String) COMMENT '企业名称',
  
  `sell_name` Nullable(String) COMMENT '销售',
  
  `date_time` Nullable(Date) DEFAULT NULL COMMENT '时间',
  
  `aci_send_num` Nullable(Int32) COMMENT '发单'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/data_report/dwd_test_replica',
                             '{replica}')
                             ORDER BY customer_id
                             SETTINGS index_granularity = 8192;
                             
-- 分布式表

CREATE TABLE IF NOT EXISTS data_report.dwd_test on
cluster hgj_clickhouse_2shards_2replicas as data_report.dwd_test_replica
ENGINE = Distributed(hgj_clickhouse_2shards_2replicas,
data_report,
dwd_test_replica,
cityHash64(customer_id));
```

## 代码
```python
#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# @Project -> File：spark_demo -> spark_clickhouse.py
# @Author ：wangxintian
# @Date   ：2022/6/27 14:34
# @Desc   ：读写clickhouse

## spark 读取clickhouse
import os
from os.path import abspath

from pyspark.sql import SparkSession

warehouse_location = abspath('spark-warehouse')
# 加载本地环境方法1
os.environ["SPARK_HOME"] = "/Users/neo/Downloads/spark-2.4.0-bin-hadoop2.6"
os.environ["SPARK_PYTHON"] = "/Users/neo/Downloads/spark-2.4.0-bin-hadoop2.6/python"


def spark_write_clickhouse(df, table_name):
    df.write \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://你的ck地址/default") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("user", "用户") \
    .option("password", "密码") \
    .option("dbtable", f"{table_name}") \
    .option("batchSize", "100000") \
    .option("isolationLevel", "NONE") \
    .mode("append") \
    .save()
    return


# 读取clickhouse
class SparkClickHouse:
    def __init__(self, spark_session):
        self.spark = spark_session
        
        # 读取clickhouse 返回dataframe
        def read_clickhouse(self, sql):
            dataframe = self.spark.read \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", "jdbc:clickhouse://你的地址/default") \
            .option("user", "用户") \
            .option("password", "密码") \
            .option("query", f"{sql}") \
            .load()
            return dataframe
        
        
        if __name__ == '__main__':
            spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("spark_read_clickhouse") \
            .config("spark.sql.warehouse.dir", warehouse_location) \
            .getOrCreate()
            sc = spark.sparkContext
            sc.setLogLevel("INFO") ## 可以自己设置log级别
            spark_clickhouse = SparkClickHouse(spark)
            sql = "select * from ocean_shipping.dwd_ka_aci "
            df = spark_clickhouse.read_clickhouse(sql)
            df.show()
            
            ## 写入 clickhouse 分布式表
            spark_write_clickhouse(df, "data_report.dwd_test")
            
            # 关闭spark
            spark.stop()
            # 结束程序
            exit(0)
            

```
可能遇到的问题
```python
 'TypeError: an integer is required (got type bytes)' 
```
可能你的python3.8版本不兼容，降级到python3.7即可。




