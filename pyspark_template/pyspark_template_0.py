import time
start_time_main = time.time()
#start_time = time.time()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
# import numpy as np
import pandas as pd
import os

spark = SparkSession.builder.appName("pyspark_general").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Job started")


spark.sql("select * from T").show(5)


print("Finished")


spark.stop()
print "Total time: ",time.time() - start_time_main
