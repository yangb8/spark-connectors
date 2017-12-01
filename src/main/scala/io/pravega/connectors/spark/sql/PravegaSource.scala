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
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, _}

import scala.collection.JavaConverters._

/**
  * A [[Source]] that reads data from Pravega.
  */
private[sql] class PravegaSource(
                                  sqlContext: SQLContext,
                                  pravegaReader: PravegaOffsetReader,
                                  sourceOptions: Map[String, String],
                                  metadataPath: String) extends Source with Logging {

  private lazy val initialPartitionOffsets = {
    sourceOptions.getOrElse("startoffset", "") match {
      case "earliest" =>
        // TODO, is there a better way to convert java Long to Scala Long?
        pravegaReader.fetchEarliestOffsets.asScala.toMap.map { case (k, v) =>
          val o: Long = v
          k -> o
        }
      case "latest" =>
        pravegaReader.fetchLatestOffsets.asScala.toMap.map { case (k, v) =>
          val o: Long = v
          k -> o
        }
      case "specific" =>
        JsonUtils.partitionOffsets(sourceOptions.getOrElse("startoffset.jsonstr", ""))
      // resume from checkpoint, spark StreamExecution will take care of startoffset
      // set to earliest offsets that's only used when starting this stream for the first time
      case _ =>
        pravegaReader.fetchEarliestOffsets.asScala.toMap.map { case (k, v) =>
          val o: Long = v
          k -> o
        }
    }
  }
  private val sc = sqlContext.sparkContext
  private var currentPartitionOffsets = Map[Segment, Long]()

  /** Returns the next available latest offset for this source. */
  override def getOffset: Option[Offset] = {
    initialPartitionOffsets

    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = initialPartitionOffsets
    }

    val latest = pravegaReader.fetchLatestOffsets.asScala.toMap.map { case (k, v) =>
      val o: Long = v
      k -> o
    }
    var newOffsets = scala.collection.mutable.Map[Segment, Long]()
    for ((segment, offset) <- currentPartitionOffsets) {
      if (offset < Long.MaxValue) {
        newOffsets(segment) = latest.getOrElse(segment, Long.MaxValue)
      } else {
        for (successor <- pravegaReader.getSuccessors(segment).asScala) {
          newOffsets(successor) = latest.getOrElse(successor, Long.MaxValue)
        }
      }
    }

    val offsets = newOffsets.map(kv => kv._1 -> kv._2).toMap
    logWarning(s"####### GetOffset: ${currentPartitionOffsets}, ${latest}, ${offsets.toSeq.map(_.toString).sorted}")
    currentPartitionOffsets = offsets
    Some(PravegaSourceOffset(offsets))
  }

  /**
    * Returns the data that is between the offsets
    * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
    * exclusive.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    /*  TODO: due to below special handling for Kafka source, last batch will be processed again after restarting.
     *  https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/StreamExecution.scala#L500-L512
     *
     *  Accordingly we need to add some special handling here to skip processing last batch to avoid duplicate events
     */
    initialPartitionOffsets

    logWarning(s"####### GetBatch: called with start = $start, end = $end")
    val untilPartitionOffsets = PravegaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        PravegaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    // Calculate offset ranges
    val offsetRanges = untilPartitionOffsets.map { case (s, o) =>
      PravegaSourceRDDOffsetRange(s, fromPartitionOffsets.getOrElse(s, 0), o, None)
    }.toArray

    val pravegaParams = new ju.HashMap[String, String](sourceOptions.filterKeys(_.startsWith("pravega.server.")).asJava)
    // Create an RDD that reads from Pravega and get the (key, value) pair as byte arrays.
    val rdd = new PravegaSourceRDD(
      sc,
      pravegaParams,
      offsetRanges).map { event =>
      Row(
        event.value,
        event.segment.getScope,
        event.segment.getStreamName,
        event.segment.getSegmentNumber,
        event.offset)
    }

    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = untilPartitionOffsets
    }

    sqlContext.createDataFrame(rdd, schema)
  }

  override def schema: StructType = PravegaSource.pravegaSchema

  /* Don't need to commit anything, we leverage Spark checkpoints to do recovery
  override def commit(end: Offset): Unit = {}
  */

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    pravegaReader.close()
  }

  override def toString(): String = s"PravegaSource[$pravegaReader]"
}

private[sql] object PravegaSource {

  def pravegaSchema: StructType = StructType(Seq(
    // TODO, do we need key?
    StructField("value", BinaryType),
    StructField("scope", StringType),
    StructField("stream", StringType),
    StructField("segment", IntegerType),
    StructField("offset", LongType)
  ))
}
