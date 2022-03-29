local_spark_location := ${SPARK_LOCATION}

# brew install sbt
build:
	sbt clean assembly

submit: build
	$(local_spark_location)/bin/spark-submit \
		--properties-file=spark-defaults.conf \
		--class com.iomete.spark.datacompaction.SqlCompaction \
		target/scala-2.12/data-compaction-job-assembly-0.1.1.jar
