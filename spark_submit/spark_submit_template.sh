#!/bin/bash

ssh user@server << EOF
 kinit -kt /usr/keytabs/user.headless.keytab user@SERVER.NAME.COM
 export SPARK_MAJOR_VERSION=2
 
 printf "\n\nSTART###" >> /home/user/log.txt
 date >> /home/user/log.txt

 spark-submit \
 --conf "spark.yarn.am.extraJavaOptions -Dhdp.version=2.6.4.0-91" --conf "spark.driver.extraJavaOptions -Dhdp.version=2.6.4.0-91" --conf "spark.authenticate=false" --conf "spark.shuffle.service.enabled=false" --conf "spark.dynamicAllocation.enabled=false" --conf "spark.network.crypto.enabled=false" --conf "spark.authenticate.enableSaslEncryption=false" --conf "spark.network.timeout=10000000" --conf "spark.executor.heartbeatInterval=10000000" --conf "spark.shuffle.registration.timeout=50000" \
 --master yarn --deploy-mode client --queue default --conf spark.driver.port=2612 --conf spark.ui.port=7914 \
 --conf "spark.driver.maxResultSize=20g" --driver-memory 20g --num-executors 200 --executor-cores 5 --executor-memory 20g \
 --conf "spark.default.parallelism=2000" --conf "spark.sql.shuffle.partitions=2000" --conf "spark.scheduler.mode=FAIR" \
 --packages org.locationtech.geotrellis:geotrellis-proj4_2.12:2.2.0 \
 /home/user/xxx.jar >> /home/user/log.txt;
 
 printf "\n\nEND " >> /home/user/log.txt
 date >> /home/user/log.txt
 
EOF

ssh -i /home/user/.ssh/id_rsa_3 user@server2 << EOF
 /usr/path/hadoop/hadoop-2.7.3/bin/hadoop fs -chmod -R 777 /path
EOF

