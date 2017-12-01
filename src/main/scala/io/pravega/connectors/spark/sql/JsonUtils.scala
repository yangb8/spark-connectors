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
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * Utilities for converting Pravega related objects to and from json.
  */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Read per-TopicPartition offsets from json string
    */
  def partitionOffsets(str: String): Map[Segment, Long] = {
    try {
      Serialization.read[Map[String, Long]](str).map { case (scopedName, offset) =>
        Segment.fromScopedName(scopedName) -> offset
      } //.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"scope1/stream1/12":10,"scope1/stream2/13":20}, got $str""")
    }
  }

  /**
    * Write per-TopicPartition offsets as json string
    */
  def partitionOffsets(partitionOffsets: Map[Segment, Long]): String = {
    val result = new HashMap[String, Long]()
    val partitions = partitionOffsets.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { segment =>
      result += segment.getScopedName -> partitionOffsets(segment)
    }
    Serialization.write(result)
  }
}
