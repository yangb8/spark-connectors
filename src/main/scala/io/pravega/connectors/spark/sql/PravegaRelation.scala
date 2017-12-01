/**
  * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  */

package io.pravega.connectors.spark.sql

import java.{util => ju}

import io.pravega.client.segment.impl.Segment
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._


// TODO, make it solid after PravegaSource work is fully done
private[sql] class PravegaRelation(
                                    override val sqlContext: SQLContext,
                                    sourceOptions: Map[String, String],
                                    startingOffsets: Map[Segment, Long],
                                    endingOffsets: Map[Segment, Long])
  extends BaseRelation with TableScan with Logging {

  override def buildScan(): RDD[Row] = {

    // TODO refine the calculation
    val offsetRanges = endingOffsets.map { case (s, o) =>
      PravegaSourceRDDOffsetRange(s, startingOffsets.getOrElse(s, 0), o, None)
    }.toArray

    val pravegaParams = new ju.HashMap[String, String](sourceOptions.filterKeys(_.startsWith("pravega.server.")).asJava)
    logInfo("######## buildScan 1 " + pravegaParams)
    // Create an RDD that reads from Pravega and get the (key, value) pair as byte arrays.
    val rdd = new PravegaSourceRDD(
      sqlContext.sparkContext,
      pravegaParams,
      offsetRanges).map { event =>
      Row(
        event.value,
        event.segment.getScope,
        event.segment.getStreamName,
        event.segment.getSegmentNumber,
        event.offset)
    }

    logInfo(s"######## buildScan 2 " + rdd)
    rdd
  }

  override def schema: StructType = PravegaSource.pravegaSchema

  override def toString: String = s"PravegaRelation(, start=$startingOffsets, end=$endingOffsets)"
}
