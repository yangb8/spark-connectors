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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
  * The provider class for the [[PravegaSource]]. This provider is designed such that it throws
  * IllegalArgumentException when the Pravega Dataset is created, so that it can catch
  * missing options even before the query is started.
  */
private[sql] class PravegaSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  // TODO with StreamSinkProvider
  with RelationProvider
  // TODO with CreatableRelationProvider
  with Logging {

  /**
    * Returns the name and schema of the source. In addition, it also verifies whether the options
    * are correct and sufficient to create the [[PravegaSource]] when the query is started.
    */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Pravega source has a fixed schema and cannot be set with a custom one")
    (shortName(), PravegaSource.pravegaSchema)
  }

  override def shortName(): String = "pravega"

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    val pravegaParams = new ju.HashMap[String, String](parameters.filterKeys(_.startsWith("pravega.server.")).asJava)
    val pravegaOffsetReader = new PravegaOffsetReader(
      pravegaParams.get("pravega.server.uri"),
      pravegaParams.get("pravega.server.scopename"),
      pravegaParams.get("pravega.server.streamnames"))

    new PravegaSource(sqlContext, pravegaOffsetReader, parameters, metadataPath)
  }

  /**
    * Returns a new base relation with the given parameters.
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    new PravegaRelation(
      sqlContext,
      parameters,
      // TODO
      Map[Segment, Long](), // startingOffsets
      Map[Segment, Long]()) // endingOffsets
  }
}
