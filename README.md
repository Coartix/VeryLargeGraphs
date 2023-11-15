Need:
Java 8
Pyspark 3.5.0
Spark 3.5.0


1) Benchmark on list computations from local to spark cluster.

2) Benchmark on BronKerbosch algorithm from local to spark cluster.

3) Ideas to improve spark cluster.

/opt/spark/bin/spark-submit --master spark://10.41.178.221:7077 --deploy-mode client --num-executors 4 --executor-cores 4 cluster.py

/opt/spark/sbin/start-slave.sh spark://10.41.178.221:7077

/opt/spark/sbin/start-master.sh

Master env on localhost:8080
