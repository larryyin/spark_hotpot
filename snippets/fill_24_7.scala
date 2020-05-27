UID.crossJoin(spark.range(1,8,1).withColumnRenamed("id","dow").crossJoin(spark.range(0,24,1).withColumnRenamed("id","hr"))),Seq("uid","dow","hr"),"right")
