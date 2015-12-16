# spark-duplicate-detection
A first attempt at using spark to find duplicates within the occurrence records. This procedure requires a parquet file 
version of the occurrence table in hdfs. Use the script in src/main/resources to create the parquet file with Hive. Note
that there are hard-coded paths in the DupeDetector class that you should change to match you preferences.

The result of this computation will be a set of files with rows like the following:

```(2952950|-9.65472|-40.66667|421279200000|Fotius, G.,1094881118|2e684fd 1086728232|afae313)```

which are serialized Tuple2 objects. The format is 
  
  ```(taxonkey|lat|lng|eventdate in ms|collectorname,gbifid|datasetkeyprefix gbifid|datasetkeyprefix ...)```
  
Note that as of time of writing (Dec 2015) the first 7 characters of the datasetkey uuid was enough to uniquely
identify it. The next step in analysis of these results would be to split up these duplicates and compare datasets pairs
that show up frequently in the hopes of identifying whole or partial datasets that have been duplicated within another
dataset (eg an aggregator is publishing a dataset that we have also indexed). 

## SBT
This is a scala project and requires the "Scala Build Tool" (sbt) for building and packaging. See their site for details
on installation: http://www.scala-sbt.org/release/tutorial/Setup.html.
 
## Spark 1.5+
This project relies on DataFrames, which are a SparkSQL feature that only really works in Spark 1.5 and up. Spark 1.5.0
is included in CDH 5.5, but in CDH 5.4 it's 1.3.x and therefore this project WILL NOT WORK. Don't even try. The brave
can build their own Spark 1.5.2 on a gateway machine using the following commandline:

```mvn clean package -DskipTests -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -Phive -Phive-thriftserver```

When running either spark-shell or spark-submit it will require configuration files for your cluster, as well as the
Hive jars. This feels like a hack, and will probably work without the hive jars in a properly setup CDH 5.5 environment.
In the meantime you can use the src/main/canned_hive_classpath.sh to set up your environment for a CDH 5.4.7 gateway
(see below).

## Running

Set your environment variables, build a "fat jar" assembly using sbt, and submit to the cluster. If you're using a stock
spark (ie you didn't build your own) it should be enough to just use the "spark-submit" binary without any explicit path.

```
source canned_hive_classpath.sh
sbt package assembly
~/spark-1.5.2/bin/spark-submit --master yarn --jars $HIVE_CLASSPATH --deploy-mode cluster --num-executors 20 --executor-memory 2g --executor-cores 1 --class "org.gbif.spark.DupeDetector" target/scala-2.10/spark-duplicate-detection-assembly.jar

```

Look in the path you set in DupeDetector for a new directory spark-duplicate-detector/<timestamp> that has your result. 


