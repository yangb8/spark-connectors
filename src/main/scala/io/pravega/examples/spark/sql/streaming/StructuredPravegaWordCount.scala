/**
  * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  */

// scalastyle:off println
package io.pravega.examples.spark.sql.streaming

import java.nio.ByteBuffer

import io.pravega.client.stream.impl.JavaSerializer
import org.apache.spark.sql.SparkSession

object StructuredPravegaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredPravegaWordCount <uri> " +
        "<scope_name> <stream_names_seperated_by_comma>")
      System.exit(1)
    }

    val Array(uri, scopename, streamnames, statestream, statekey) = args

    val spark = SparkSession
      .builder
      .appName("StructuredPravegaWordCount")
      .getOrCreate()

    import spark.implicits._

    val deserializer = new JavaSerializer[String]
    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      deserializer.deserialize(ByteBuffer.wrap(bytes))
    )

    // Create DataSet representing the stream of input lines from pravega
    val lines = spark
      .readStream
      .format("pravega")
      .option("pravega.server.uri", uri)
      .option("pravega.server.scopename", scopename)
      .option("pravega.server.streamnames", streamnames)
      .option("startoffset", "earliest")
      .load()
      .selectExpr("""deserialize(value)""")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .queryName("aggregates")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

// scalastyle:on println
