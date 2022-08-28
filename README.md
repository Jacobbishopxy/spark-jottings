# Spark Jotting

## Cluster mode

Cluster mode means `spark-submit --master k8s://${MASTER_ADDR} ...`.

- Since we are using k8s spark cluster (see [detail](https://stackoverflow.com/a/68779353)), we need [bcpkix-jdk15on](https://mvnrepository.com/artifact/org.bouncycastle/bcpkix-jdk15on) & [bcprov-jdk15on](https://mvnrepository.com/artifact/org.bouncycastle/bcprov-jdk15on) for `spark-submit`. In other words, these two dependencies must be included in `$SPARK_HOME/jars` (Note: run `echo 'sc.getConf.get("spark.home")' | spark-shell` to find out `$SPARK_HOME` if needed).

- In addition, we need [hadoop-aws](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws) as an extra package while executing `spark-submit`.

- Check username/password of a deployed standalone MinIO.

  persistent depends on your Volume, login to your node IP then:

  ```sh
  cat <minio path>/.root_user
  cat <minio path>/.root_password
  ```

- [k8s spark cluster job cleaner](https://github.com/dtan4/k8s-job-cleaner)

## Client mode

Client mode means using [bitnami/charts](https://github.com/bitnami/charts/tree/master/bitnami/spark) in k8s.

- NFS share volume (Only required in Spark Client Mode, which used for uploading local JARs).

  On the client server:

  ```sh
  sudo apt update
  sudo apt install nfs-common
  ```

  Check available mounting directories:

  ```sh
  showmount -e <HOST_IP>
  ```

  Make the share directory and grant permission:

  ```sh
  sudo mkdir <YOUR_MOUNT_DIRECTORY> -p
  sudo chown nobody:nogroup <YOUR_MOUNT_DIRECTORY>
  ```

  Mount host directory:

  ```sh
  sudo mount <HOST_IP>:<HOST_SHARE_ADDRESS> <YOUR_MOUNT_DIRECTORY>
  ```

## Utilities

- accessing logs:

  ```sh
  kubectl logs -f -n dev <DRIVER_POD_NAME>
  ```

- accessing UI

  ```sh
  kubectl port-forward -n dev <DRIVER_POD_NAME> 4040:4040
  ```

- debugging

  ```sh
  kubectl describe pod -n dev <SPARK_DRIVER_POD>
  ```

- killing driver

  ```sh
  kubectl describe pod -n dev <SPARK_DRIVER_POD>
  ```

## Examples

- [RDDRelation](./src/main/scala/example/RDDRelation.scala): RDD
- [DataSources](./src/main/scala/example/DataSources.scala): data sources, such as CSV, Parquet, Jdbc, and etc.
- [TypedAggregator](./src/main/scala/example/TypedAggregator.scala): simple aggregator
- [UserDefinedTypedAggregator](./src/main/scala/example/UserDefinedTypedAggregator.scala): user defined aggregator
- [UserDefinedUntypedAggregator.](./src/main/scala/example/UserDefinedUntypedAggregator.scala): user defined aggregator and with `spark.sql`
- [DFWithColumn](./src/main/scala/example/DFWithColumn.scala): `withColumn` function
- [DFWhereFilter](./src/main/scala/example/DFWhereFilter.scala): `filter` & `where` clauses
- [DFWhen](./src/main/scala/example/DFWhen.scala): "case when" and "when otherwise"

## Notes

- `--jars` are used for local or remote jar files specified with URL and don't resolve dependencies, `--packages` are used for Maven coordinates, and do resolve dependencies. _[Source](https://stackoverflow.com/a/50334235)_

## Materials

- [Running Apache Spark on Kubernetes](https://medium.com/empathyco/running-apache-spark-on-kubernetes-2e64c73d0bb2)
- [Apache Spark: Differences between client and cluster deploy modes](https://stackoverflow.com/questions/37027732/apache-spark-differences-between-client-and-cluster-deploy-modes)
