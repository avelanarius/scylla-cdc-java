package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import com.scylladb.cdc.transport.WorkerTransport;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import static org.junit.jupiter.api.Assertions.fail;

public class WorkerThread implements AutoCloseable {
    public static final long DEFAULT_QUERY_WINDOW_SIZE_MS = 5;
    public static final long DEFAULT_CONFIDENCE_WINDOW_SIZE_MS = 10;

    private static final long THREAD_JOIN_TIMEOUT_MS = 3000;
    private final Worker worker;
    private final Thread workerThread;

    public WorkerThread(WorkerConfiguration workerConfiguration, Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        Preconditions.checkNotNull(workerConfiguration);
        this.worker = new Worker(workerConfiguration);
        this.workerThread = new Thread(() -> {
            try {
                worker.run(groupedStreams);
            } catch (Exception e) {
                // FIXME - this does not fail the test!
                fail("Exception in run() of worker", e);
            }
        });
        this.workerThread.start();
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer, Clock clock,
                        Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        this(WorkerConfiguration.builder()
                .withCQL(workerCQL)
                .withTransport(workerTransport)
                .withConsumer(consumer)
                .withQueryTimeWindowSizeMs(DEFAULT_QUERY_WINDOW_SIZE_MS)
                .withConfidenceWindowSizeMs(DEFAULT_CONFIDENCE_WINDOW_SIZE_MS)
                .withClock(clock)
                .build(), groupedStreams);
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer,
                        GenerationMetadata generationMetadata, Clock clock, Set<TableName> tableNames) {
        this(workerCQL, workerTransport, consumer, clock, MockGenerationMetadata.toMap(generationMetadata, tableNames));
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer,
                        GenerationMetadata generationMetadata, TableName tableName) {
        this(workerCQL, workerTransport, consumer, generationMetadata, Clock.systemDefaultZone(), tableName);
    }

    public WorkerThread(WorkerCQL workerCQL, WorkerTransport workerTransport, Consumer consumer,
                        GenerationMetadata generationMetadata, Clock clock, TableName tableName) {
        this(workerCQL, workerTransport, consumer, generationMetadata, clock, Collections.singleton(tableName));
    }

    @Override
    public void close() {
        if (this.worker != null) {
            this.worker.stop();
        }
        if (this.workerThread != null) {
            try {
                this.workerThread.join(THREAD_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                fail("Could not successfully join() worker thread", e);
            } finally {
                if (this.workerThread.isAlive()) {
                    fail("Could not successfully close worker thread");
                }
            }
        }
    }
}
