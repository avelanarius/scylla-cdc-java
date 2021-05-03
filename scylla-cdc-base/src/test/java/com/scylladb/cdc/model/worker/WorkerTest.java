package com.scylladb.cdc.model.worker;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import com.scylladb.cdc.transport.MockWorkerTransport;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.scylladb.cdc.model.worker.WorkerThread.DEFAULT_QUERY_WINDOW_SIZE_MS;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.*;

public class WorkerTest {
    private static final long DEFAULT_AWAIT_TIMEOUT_MS = 2000;
    private static final ConditionFactory DEFAULT_AWAIT =
            with().pollInterval(1, TimeUnit.MILLISECONDS).await()
                    .atMost(DEFAULT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    private static long TEST_GENERATION_START_MS = 5 * 60 * 1000;
    private static GenerationMetadata TEST_GENERATION = MockGenerationMetadata.mockGenerationMetadata(
            new Timestamp(new Date(TEST_GENERATION_START_MS)), Optional.empty(), 16);

    private static TableName TEST_TABLE_NAME = new TableName("ks", "t");

    // ChangeSchema for table:
    // CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true};
    private static ChangeSchema TEST_CHANGE_SCHEMA = new ChangeSchema(Lists.newArrayList(
            new ChangeSchema.ColumnDefinition("cdc$stream_id", 0, new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB), null, null, false),
            new ChangeSchema.ColumnDefinition("cdc$time", 1, new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID), null, null, false),
            new ChangeSchema.ColumnDefinition("cdc$batch_seq_no", 2, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), null, null, false),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null, false),
            new ChangeSchema.ColumnDefinition("cdc$end_of_batch", 4, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null, false),
            new ChangeSchema.ColumnDefinition("cdc$operation", 5, new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT), null, null, false),
            new ChangeSchema.ColumnDefinition("cdc$ttl", 6, new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT), null, null, false),
            new ChangeSchema.ColumnDefinition("ck", 7, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnType.CLUSTERING_KEY, false),
            new ChangeSchema.ColumnDefinition("pk", 8, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnType.PARTITION_KEY, false),
            new ChangeSchema.ColumnDefinition("v", 9, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnType.REGULAR, false)
    ));

    @Test
    public void testWorkerReadsAnyWindows() {
        // Worker should start reading windows
        // from the beginning of the generation.

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));

            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS,
                    TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS)));

            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                    TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS)));

            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 3, TEST_TABLE_NAME,
                    TEST_GENERATION_START_MS, TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS)));
        }
    }

    @Test
    public void testWorkerTrimsWindowWithTTL() {
        // If a TTL was set on a table (as is the case
        // in Scylla by default on the CDC log table), Worker
        // should not start reading from the beginning of
        // generation, but from |now - ttl|.

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();

        // Setting a 146 second TTL:
        mockWorkerCQL.setTablesTTL(Collections.singletonMap(TEST_TABLE_NAME, Optional.of(146L)));

        // "Now" will be at second 935:
        Clock clock = Clock.fixed(Instant.ofEpochSecond(935), ZoneOffset.systemDefault());

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, clock, TEST_TABLE_NAME)) {

            // Expecting to see a window queried from time point (now - ttl) = (935 - 146).
            // Multiplying by 1000 to convert from seconds to milliseconds.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (935 - 146) * 1000, (935 - 146) * 1000 + DEFAULT_QUERY_WINDOW_SIZE_MS)));

            // And no window before that:
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (935 - 146) * 1000 - DEFAULT_QUERY_WINDOW_SIZE_MS, (935 - 146) * 1000)));
        }
    }

    @Test
    public void testWorkerTrimsSavedTaskStatesWithTTL() {
        // If a TTL is set on a table, but
        // there are TaskStates saved on the transport,
        // they still should be trimmed with the TTL value.

        // "Now" will be at second 935:
        long now = 935, ttl = 146;
        Clock clock = Clock.fixed(Instant.ofEpochSecond(now), ZoneOffset.systemDefault());

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        // Previously saved TaskStates:
        // task1 - before TTL
        // task2 - after TTL
        // task3 - intersecting with TTL
        Task task1 = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 18 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 19 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        Task task2 = generateTask(TEST_GENERATION, 1, TEST_TABLE_NAME,
                900 * 1000, 900 * 1000 + DEFAULT_QUERY_WINDOW_SIZE_MS);
        Task task3 = generateTask(TEST_GENERATION, 2, TEST_TABLE_NAME,
                (now - ttl) * 1000 - 3,
                (now - ttl) * 1000 - 3 + DEFAULT_QUERY_WINDOW_SIZE_MS);
        // FIXME: add a test with TaskState with lastConsumedId

        workerTransport.setState(task1.id, task1.state);
        workerTransport.setState(task2.id, task2.state);
        workerTransport.setState(task3.id, task3.state);

        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();

        // Setting a 146 second TTL:
        mockWorkerCQL.setTablesTTL(Collections.singletonMap(TEST_TABLE_NAME, Optional.of(ttl)));

        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, clock, TEST_TABLE_NAME)) {
            // task1 - before TTL, so Worker should start reading
            // from TTL, "discarding" task1.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (now - ttl) * 1000, (now - ttl) * 1000 + DEFAULT_QUERY_WINDOW_SIZE_MS)));
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                    (now - ttl) * 1000 - DEFAULT_QUERY_WINDOW_SIZE_MS, (now - ttl) * 1000)));

            // task2 - after TTL, so Worker should start reading
            // from it.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(task2));
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(task2.updateState(task2.state.moveToNextWindow(-DEFAULT_QUERY_WINDOW_SIZE_MS))));

            // task3 - intersecting with TTL. For simplicity,
            // the Worker does not trim it and starts from it.
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.isReaderFinished(task3));
            assertFalse(mockWorkerCQL.wasCreateReaderInvoked(task3.updateState(task3.state.moveToNextWindow(-DEFAULT_QUERY_WINDOW_SIZE_MS))));
        }
    }

    @Test
    public void testWorkerConsumesSingleVNodeChangesInOrder() {
        // Worker should consume changes within a single
        // vnode in order of stream id and timestamp.

        MockRawChange change1 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 12,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 2, "v", 3));

        MockRawChange change2 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 1, TEST_GENERATION_START_MS + 12,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 4, "ck", 5, "v", 6));

        MockRawChange change3 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 2, TEST_GENERATION_START_MS + 13,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 7, "ck", 8, "v", 9));

        MockRawChange change4 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 19,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 10, "ck", 11, "v", 12));

        MockRawChange change5 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 3, TEST_GENERATION_START_MS + 100,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 13, "ck", 14, "v", 15));

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2, change3, change4, change5);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }
    }

    @Test
    public void testWorkerConsumesMultiVNodeChanges() {
        // Worker should be able to read changes
        // from all vnodes, many windows.

        List<MockRawChange> rawChanges = new ArrayList<>();
        for (int vnode = 0; vnode < 16; vnode++) {
            rawChanges.add(generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                    vnode, 2, TEST_GENERATION_START_MS + 17,
                    ImmutableMap.of("cdc$deleted_v", false, "pk", 15, "ck", 1, "v", 7)));

            rawChanges.add(generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                    vnode, 1, TEST_GENERATION_START_MS + 17,
                    ImmutableMap.of("cdc$deleted_v", false, "pk", 11, "ck", vnode, "v", 3)));

            rawChanges.add(generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                    vnode, 1, TEST_GENERATION_START_MS + 44,
                    ImmutableMap.of("cdc$deleted_v", false, "pk", vnode, "ck", 2, "v", 3)));

            rawChanges.add(generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                    vnode, 3, TEST_GENERATION_START_MS + 48 + vnode * 3,
                    ImmutableMap.of("cdc$deleted_v", false, "pk", 12, "ck", 5, "v", vnode)));
        }

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.containsAll(rawChanges));
            // No duplicates:
            assertEquals(observedChanges.size(), rawChanges.size());
        }
    }

    @Test
    public void testWorkerConsumesChangesFromSavedTaskState() {
        // Worker should start reading changes
        // from the TaskState in Transport,
        // and skip changes before that state.

        MockRawChange change1 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 12,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 2, "v", 3));

        MockRawChange change2 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 1, TEST_GENERATION_START_MS + 12,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 4, "ck", 5, "v", 6));

        MockRawChange change3 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 2, TEST_GENERATION_START_MS + 13,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 7, "ck", 8, "v", 9));

        MockRawChange change4 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 19,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 10, "ck", 11, "v", 12));

        MockRawChange change5 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 3, TEST_GENERATION_START_MS + 100,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 13, "ck", 14, "v", 15));

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        Task savedTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 4 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        workerTransport.setState(savedTask.id, savedTask.state);

        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2, change3, change4, change5);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges.subList(3, 4)));
        }

        // Test with another savedTask:
        savedTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        savedTask = savedTask.updateState(savedTask.state.update(change1.getId()));
        workerTransport.setState(savedTask.id, savedTask.state);
        observedChanges.clear();

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges.subList(1, 4)));
        }
    }

    @Test
    public void testWorkerSavesMovedWindowStateToTransport() {
        // Worker should call save state after each
        // moving of window.

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        Consumer noOpConsumer = Consumer.syncRawChangeConsumer(c -> {});

        // The first window is not saved using moveStateToNextWindow.
        Task secondWindow = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + DEFAULT_QUERY_WINDOW_SIZE_MS, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        Task thirdWindow = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS, TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, noOpConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            // Wait for the third window to be committed to transport.
            DEFAULT_AWAIT.until(() -> workerTransport.getMoveStateToNextWindowInvocations(thirdWindow.id).size() >= 2);
        }

        // Worker has now stopped, check if the transport
        // received moveStateToNextWindow for the second
        // and third window.

        List<TaskState> windows = workerTransport.getMoveStateToNextWindowInvocations(secondWindow.id).subList(0, 2);
        assertEquals(Lists.newArrayList(secondWindow.state, thirdWindow.state), windows);
    }

    @Test
    public void testWorkerSavesWithinWindowStateToTransport() {
        // Worker should call save state after
        // each successful reading of a change.

        MockRawChange change1 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 2, "v", 3));

        MockRawChange change2 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 4, "ck", 5, "v", 6));

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }

        // The consumer has received all changes,
        // let's check whether it updated the windows correctly.
        Task windowReadTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);

        // Skip a few windows unrelated to windowReadTask:
        List<TaskState> windows = workerTransport.getSetStateInvocations(windowReadTask.id).subList(2, 5);

        TaskState windowBeginningState = windowReadTask.state;
        TaskState afterChange1State = windowReadTask.state.update(change1.getId());
        TaskState afterChange2State = windowReadTask.state.update(change2.getId());

        assertEquals(Lists.newArrayList(windowBeginningState, afterChange1State, afterChange2State), windows);
    }

    @Test
    public void testWorkerRetriesFailedConsumer() {
        // Test that Worker retries after
        // exception from Consumer and
        // doesn't re-read changes.

        MockRawChange change1 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 2, "v", 3));

        MockRawChange change2 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 4, "ck", 5, "v", 6));

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2);
        mockWorkerCQL.setRawChanges(rawChanges);

        // Set up a consumer that starts
        // failing after the first change
        // until failure mode is stopped.
        AtomicBoolean shouldFail = new AtomicBoolean(false);
        AtomicInteger failCount = new AtomicInteger(0);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer failingConsumer = Consumer.forRawChangeConsumer(change -> {
            if (shouldFail.get()) {
                failCount.incrementAndGet();
                CompletableFuture<Void> injectedExceptionFuture = new CompletableFuture<>();
                injectedExceptionFuture.completeExceptionally(new RuntimeException("Injected exception in failingConsumer"));
                return injectedExceptionFuture;
            }
            if (change.equals(change1)) {
                // Start failing after the first change
                shouldFail.set(true);
            }
            observedChanges.add(change);
            return CompletableFuture.completedFuture(null);
        });

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, failingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> failCount.get() > 3);

            // We should have only succeeded in reading the first change.
            assertEquals(Collections.singletonList(change1), observedChanges);

            shouldFail.set(false);

            DEFAULT_AWAIT.until(() -> rawChanges.equals(observedChanges));
        }
    }

    @Test
    public void testWorkerRetriesSingleCQLException() {
        // Test that Worker correctly handles
        // a single WorkerCQL nextChange() exception.

        MockRawChange change1 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 2, "v", 3));

        MockRawChange change2 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 4, "ck", 5, "v", 6));

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        // Inject a single failure.
        mockWorkerCQL.injectRawChangeFailure(change1);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getFailureCount() == 1);
            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }
    }

    @Test
    public void testWorkerSurvivesFailureAndRestart() {
        // Test that Worker correctly handles the following
        // scenario: successful reading of a few changes,
        // then constant CQL failure, restart and successful
        // reading of next changes.

        MockRawChange change1 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 1,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 2, "v", 3));

        MockRawChange change2 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 2,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 4, "ck", 5, "v", 6));

        MockRawChange change3 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 3,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 1, "ck", 9, "v", 4));

        MockRawChange change4 = generateRawChange(TEST_GENERATION, TEST_CHANGE_SCHEMA,
                0, 0, TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS + 4,
                ImmutableMap.of("cdc$deleted_v", false, "pk", 2, "ck", 9, "v", 11));

        MockWorkerTransport workerTransport = new MockWorkerTransport();
        MockWorkerCQL mockWorkerCQL = new MockWorkerCQL();
        List<MockRawChange> rawChanges = Lists.newArrayList(change1, change2, change3, change4);
        mockWorkerCQL.setRawChanges(rawChanges);
        List<RawChange> observedChanges = Collections.synchronizedList(new ArrayList<>());
        Consumer accumulatingConsumer = Consumer.syncRawChangeConsumer(observedChanges::add);

        // Inject a "constant" failure at change 3.
        mockWorkerCQL.injectConstantRawChangeFailure(change3);

        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getFailureCount() > 2);

            // We will only successfully have read the first two changes.
            DEFAULT_AWAIT.until(() -> observedChanges.equals(Lists.newArrayList(change1, change2)));
        }

        // Check if the transport has the correct TaskState.
        Task failedTask = generateTask(TEST_GENERATION, 0, TEST_TABLE_NAME,
                TEST_GENERATION_START_MS + 2 * DEFAULT_QUERY_WINDOW_SIZE_MS,
                TEST_GENERATION_START_MS + 3 * DEFAULT_QUERY_WINDOW_SIZE_MS);
        TaskState fetchedTaskState = workerTransport.getTaskStates(Collections.singleton(failedTask.id)).get(failedTask.id);
        assertEquals(fetchedTaskState, failedTask.state.update(change2.getId()));

        // Restart Worker.
        try (WorkerThread workerThread = new WorkerThread(
                mockWorkerCQL, workerTransport, accumulatingConsumer, TEST_GENERATION, TEST_TABLE_NAME)) {
            // Let if fail a few times more.

            int initialFailureCount = mockWorkerCQL.getFailureCount();
            DEFAULT_AWAIT.until(() -> mockWorkerCQL.getFailureCount() > initialFailureCount + 2);

            // De-inject a failure, making it possible to read all changes.
            mockWorkerCQL.deinjectConstantRawChangeFailure(change3);

            DEFAULT_AWAIT.until(() -> observedChanges.equals(rawChanges));
        }
    }

    private Task generateTask(GenerationMetadata generationMetadata, int vnodeIndex, TableName tableName,
                              long windowStartMs, long windowEndMs) {
        VNodeId vnodeId = new VNodeId(vnodeIndex);
        TaskId taskId = new TaskId(generationMetadata.getId(), vnodeId, tableName);

        SortedSet<StreamId> streamIds = generationMetadata.getStreams().stream()
                .filter(s -> s.getVNodeId().equals(vnodeId))
                .collect(Collectors.toCollection(TreeSet::new));

        TaskState taskState = new TaskState(new Timestamp(new Date(windowStartMs)),
                new Timestamp(new Date(windowEndMs)), Optional.empty());
        return new Task(taskId, streamIds, taskState);
    }

    private MockRawChange generateRawChange(GenerationMetadata generationMetadata, ChangeSchema changeSchema,
                                            int vnodeNumber, int streamNumber, long timestamp,
                                            Map<String, Object> columnValues) {
        ByteBuffer streamId = generationMetadata.getStreams().stream()
                .filter(s -> s.getVNodeId().getIndex() == vnodeNumber)
                .skip(streamNumber).findFirst().get().getValue();
        return new MockRawChange(changeSchema, new HashMap<String, Object>() {{
            put("cdc$stream_id", streamId);
            put("cdc$time", TimeUUID.middleOf(timestamp));
            put("cdc$batch_seq_no", 0);
            put("cdc$end_of_batch", true);
            put("cdc$operation", RawChange.OperationType.ROW_INSERT.operationId);
            put("cdc$ttl", null);
            putAll(columnValues);
        }});
    }
}
