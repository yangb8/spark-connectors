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

import java.net.URI
import java.{util => ju}

import io.pravega.client.batch.BatchClient
import io.pravega.client.ClientFactory
import io.pravega.client.segment.impl.{EndOfSegmentException, Segment}
import io.pravega.client.stream.impl.ByteArraySerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/** Offset range that one partition of the PravegaSourceRDD has to read */
private[sql] case class PravegaSourceRDDOffsetRange(segment: Segment,
                                                    startOffset: Long,
                                                    endOffset: Long,
                                                    preferredLoc: Option[String])

/** Partition of the PravegaSourceRDD */
private[sql] case class PravegaSourceRDDPartition(index: Int, offsetRange: PravegaSourceRDDOffsetRange) extends Partition

/** Event read from Pravega */
private[sql] case class PravegaEventRead(value: Array[Byte], segment: Segment, offset: Long)

/**
  * An RDD that reads data from Pravega based on offset ranges across multiple segments.
  *
  * @param sc            the [[SparkContext]]
  * @param pravegaParams Pravega configuration for creating Iterator to read events
  * @param offsetRanges  Offset ranges that define the Pravega data belonging to this RDD
  */
private[sql] class PravegaSourceRDD(sc: SparkContext,
                                    pravegaParams: ju.Map[String, String],
                                    offsetRanges: Seq[PravegaSourceRDDOffsetRange])
  extends RDD[PravegaEventRead](sc, Nil) {

  /** TODO, non critical for PoC
    * investigate whether need to and how to implement
    * override def persist(newLevel: StorageLevel): this.type = { super.persist(newLevel) }
    */

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (offsetRange, index) =>
      new PravegaSourceRDDPartition(index, offsetRange)
    }.toArray
  }


  /** TODO, non critical for PoC, and seems like everything is working w/o it, need investigation
    * override def count(): Long = ?
    * override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble]
    */
  override def isEmpty(): Boolean = count == 0L

  /** TODO, non critical for PoC
    * override def take(num: Int): Array[PravegaEventRead]
    */

  /** TODO, non critical for PoC
    * override def getPreferredLocations(split: Partition): Seq[String]
    */

  override def compute(split: Partition, context: TaskContext): PravegaSegmentIterator = {
    val pravegaPartition = split.asInstanceOf[PravegaSourceRDDPartition]
    new PravegaSegmentIterator(
      context,
      pravegaParams.get("pravega.server.uri"),
      pravegaPartition.offsetRange.segment,
      pravegaPartition.offsetRange.startOffset,
      pravegaPartition.offsetRange.endOffset)
  }
}

private[sql] class PravegaSegmentIterator(val context: TaskContext,
                                          uri: String,
                                          segment: Segment,
                                          startOffset: Long,
                                          endOffset: Long) extends Iterator[PravegaEventRead] {

  private var closed = false
  private var initialized = false
  //private var clientFactory: ClientFactory = null
  //private var batchClient: BatchClient = null


  private var reader: PravegaOffsetReader = null

  lazy val segmentIterator = {
    initialized = true
    //clientFactory = ClientFactory.withScope(segment.getScope, URI.create(uri))
    //batchClient = clientFactory.createBatchClient()
    //batchClient.readSegment(segment, new ByteArraySerializer, startOffset, endOffset)
    //batchClient.readSegment(segment, new ByteArraySerializer, startOffset)
    reader = new PravegaOffsetReader(uri, segment.getScope, segment.getStreamName)
    reader.readSegment(segment, new ByteArraySerializer, startOffset, endOffset)
  }

  context.addTaskCompletionListener(context => closeIfNeeded)

  override def next: PravegaEventRead = {
    if (!hasNext) {
      throw new NoSuchElementException("End of segment")
    }
    try {
      PravegaEventRead(segmentIterator.next, segment, segmentIterator.getOffset)
    } catch {
      // happens only when startOffset is set to Long.MaxValue
      case e: EndOfSegmentException => throw new NoSuchElementException("End of segment")
    }
  }

  override def hasNext: Boolean = segmentIterator.hasNext

  def closeIfNeeded: Unit = {
    if (!closed) {
      close
      closed = true
    }
  }

  protected def close = {
    if (initialized) {
      segmentIterator.close
      reader.close
    }
  }
}
