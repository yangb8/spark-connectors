# spark-connectors
Apache Spark connectors for Pravega.

Description
-----------

Implementation of Pravega Data Source for Spark.

Build
-------

### Building Pravega

Install the Pravega client libraries to your local Maven repository:
```
$ git clone https://github.com/pravega/pravega.git
$./gradlew install
```

### Building Connector
```
gradle build (w/o dependencies)
gradle shadowJar (w/ dependencies)
```

Test
-------
```
# TODO
gradle test
```

Usage
-----
```
    // same Serializer class which was used to write to Pravega
    val deserializer = new JavaSerializer[String]
    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      deserializer.deserialize(ByteBuffer.wrap(bytes))
    )

    // Create DataSet representing the stream of input lines from Pravega
    val lines = spark
      .readStream
      .format("pravega")
      .option("pravega.server.uri", uri)                     // uri. e.g. tcp://192.168.0.200:9090
      .option("pravega.server.scopename", scopename)         // scope name
      .option("pravega.server.streamnames", streamnames)     // stream names separated by comma
      .option("startoffset", "earliest")
      // or .option("startoffset", "latest")
      // or .option("startoffset", "specific").option("startoffset.jsonstr", "<offsets_in_json>")
      // NOTE: startoffset is ignored if option("checkpointLocation", "<checkpoint_dir>") is set;
      // will start processing from saved checkpoint in checkpointLocation
      .load()
      .selectExpr("""deserialize(value)""")
      .as[String]


    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .queryName("aggregates")
      .format("console")
      // .option("checkpointLocation", "<checkpoint_dir>")  # NOTE: "console" doesn't support checkpoint, choose other options like format("memory")
      .start()
```

Run Examples
---
```
Spark (verified with Spark 2.2.0 on Ubuntu 16.04)

spark-submit --conf spark.driver.userClassPathFirst=true --class io.pravega.examples.spark.sql.streaming.StructuredPravegaWordCount build/libs/spark-connectors-0.3.0-SNAPSHOT-all.jar tcp://192.168.0.200:9090 myScope myStream myState myKey
```
