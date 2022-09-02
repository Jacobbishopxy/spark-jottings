ifneq (,$(wildcard ./.env))
    include .env
    export
endif


cluster-info:
	kubectl cluster-info

package:
	cd .. && sbt package

assembly:
	cd .. && sbt assembly

send-app: package
	scp -rp ${APP_DIR} ${SHARE_ADDR}

send-conf:
	scp -rp ${CONF_DIR} ${SHARE_ADDR}


kill-all:
	spark-submit \
		--master k8s://${MASTER_ADDR} \
		--name tmp-job \
		--kill spark:tmp-job

test-standalone-mode-job1:
	spark-submit \
		--master local[4] \
		--class jotting.simple.Job1 \
		${APP_DIR}

# TODO: temporary unavailable
# BUG: `Caused by: javax.security.auth.login.LoginException: java.lang.NullPointerException: invalid null input: name`
# -- Spark cluster mode
# -- MinIO used
test-cluster-mode-job1:
	spark-submit \
		--master k8s://${MASTER_ADDR} \
		--deploy-mode cluster \
		--name job1 \
		--class jotting.simple.Job1 \
		--packages org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.kubernetes.file.upload.path=s3a://${S3_BUCKET} \
		--conf spark.kubernetes.container.image=${K8S_CONTAINER_IMAGE} \
		--conf spark.kubernetes.namespace=${K8S_NAMESPACE} \
		--conf spark.hadoop.fs.s3a.endpoint=${SPARK_HADOOP_ENDPOINT} \
		--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
		--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		--conf spark.hadoop.fs.s3a.fast.upload=true \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.hadoop.fs.s3a.access.key=${SPARK_HADOOP_ACCESS_KEY} \
		--conf spark.hadoop.fs.s3a.secret.key=${SPARK_HADOOP_SECRET_KEY} \
		file://${PROJECT_PATH}/${APP_DIR}


# -- Spark client mode (bitnami/spark charts)
# -- nfs share used
test-client-mode-job1:
	kubectl run \
		--namespace ${K8S_NAMESPACE} job1 \
		--rm \
		--tty -i \
		--restart Never \
		--image ${K8S_CONTAINER_IMAGE} \
		-- \
		spark-submit \
		--master ${SPARK_ADDR} \
		--deploy-mode cluster \
		--class jotting.simple.Job1 \
		${SHARE_APP}

test-standalone-mode-job2:
	spark-submit \
		--master local[4] \
		--class jotting.simple.Job2 \
		--packages com.typesafe:config:1.4.2 \
		--conf spark.driver.extraLibraryPath=${DEPS_DIR} \
		${APP_DIR}

# NOTE:
# `--files` & `--jars` require `make send-conf` & `make send-jars`, respectfully
test-client-mode-job2:
	kubectl run \
		--namespace ${K8S_NAMESPACE} job2 \
		--rm \
		--tty -i \
		--restart Never \
		--image ${K8S_CONTAINER_IMAGE} \
		-- \
		spark-submit \
		--master ${SPARK_ADDR} \
		--deploy-mode cluster \
		--files ${SHARE_FILES} \
		--packages com.typesafe:config:1.4.2 \
		--jars ${SHARE_JARS_POSTGRES} \
		--class jotting.simple.Job2 \
		${SHARE_APP}

test-client-mode-job3:
	kubectl run \
		--namespace ${K8S_NAMESPACE} job3 \
		--rm \
		--tty -i \
		--restart Never \
		--image ${K8S_CONTAINER_IMAGE} \
		-- \
		spark-submit \
		--master ${SPARK_ADDR} \
		--deploy-mode cluster \
		--files ${SHARE_FILES} \
		--packages com.typesafe:config:1.4.2 \
		--jars ${SHARE_JARS_POSTGRES} \
		--class jotting.simple.Job3 \
		${SHARE_APP}
