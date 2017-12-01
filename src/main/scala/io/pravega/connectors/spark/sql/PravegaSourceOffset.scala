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

import io.pravega.client.segment.impl.Segment
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
  * An [[Offset]] for the [[PravegaSource]]. This one tracks all partitions of subscribed topics and
  * their offsets.
  */
private[sql]
case class PravegaSourceOffset(partitionToOffsets: Map[Segment, Long]) extends Offset {

  override val json = JsonUtils.partitionOffsets(partitionToOffsets)
}

/** Companion object of the [[PravegaSourceOffset]] */
private[sql] object PravegaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[Segment, Long] = {
    offset match {
      case o: PravegaSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => PravegaSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to PravegaSourceOffset")
    }
  }

  /**
    * Returns [[PravegaSourceOffset]] from a JSON [[SerializedOffset]]
    */
  def apply(offset: SerializedOffset): PravegaSourceOffset =
    PravegaSourceOffset(JsonUtils.partitionOffsets(offset.json))

  /**
    * Returns [[PravegaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
    * tuples.
    */
  def apply(offsetTuples: (Segment, Long)*): PravegaSourceOffset = {
    PravegaSourceOffset(offsetTuples.map { case (s, o) => s -> o }.toMap)
  }
}
