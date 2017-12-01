/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.spark.sql;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.batch.impl.SegmentIteratorImpl;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.impl.*;
import io.pravega.client.stream.Serializer;
import lombok.Cleanup;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;


public class PravegaOffsetReader {

    private final String scopeName;
    private final String[] streamNames;
    private final ConnectionFactory connectionFactory;
    private final Controller controller;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;

    public PravegaOffsetReader(String uri, String scopeName, String streamNames) {
        this.scopeName = scopeName;
        this.streamNames = streamNames.split(",");
        this.connectionFactory = new ConnectionFactoryImpl(false);
        final URI controllerURI = URI.create(uri);
        this.controller = new ControllerImpl(controllerURI, ControllerImplConfig.builder().build(), this.connectionFactory.getInternalExecutor());
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(this.controller, this.connectionFactory);
    }

    public Map<Segment, Long> fetchLatestOffsets() {
        Map<Segment, Long> offsets = new HashMap<>();
        for (String streamName : streamNames) {
            StreamSegments currentSegments = getAndHandleExceptions(controller.getCurrentSegments(scopeName, streamName),
                    RuntimeException::new);
            // TODO: to investigate whether pravega client has some existing API to retreive current segments' offsets in one shot
            // Anway, the current implementation shall be good enough for POC
            for (Segment segment : currentSegments.getSegments()) {
                @Cleanup
                SegmentMetadataClient metadataClient = segmentMetadataClientFactory.createSegmentMetadataClient(segment);
                offsets.put(segment, metadataClient.fetchCurrentSegmentLength());
            }
        }
        return offsets;
    }

    public Map<Segment, Long> fetchEarliestOffsets() {
        Map<Segment, Long> offsets = new HashMap<>();
        for (String streamName : streamNames) {
            Map<Segment, Long> segments = getAndHandleExceptions(controller.getSegmentsAtTime(new StreamImpl(scopeName, streamName), 0L),
                    RuntimeException::new);
            for (Segment segment : segments.keySet()) {
                offsets.put(segment, 0L);
            }
        }
        return offsets;
    }

    public Set<Segment> getSuccessors(Segment segment) {
        return getAndHandleExceptions(controller.getSuccessors(segment), RuntimeException::new).getSegmentToPredecessor().keySet();
    }

	/* We leverage Spark checkpoints now
    public String getCheckpointOffsets() {
        // TODO, do we need to refresh first?
        return config.getProperty(stateKey);
    }

    public void updateCheckpointOffsets(String value) {
        config.putProperty(stateKey, value);
    }
	*/

    public <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer, long startingOffset, long endingOffset) {
        return new SegmentIteratorImpl<>(new SegmentInputStreamFactoryImpl(controller, connectionFactory), segment, deserializer, startingOffset, endingOffset);
    }

    public void close() {
        if (controller != null) {
            controller.close();
        }
        if (connectionFactory != null) {
            connectionFactory.close();
        }
    }
}
