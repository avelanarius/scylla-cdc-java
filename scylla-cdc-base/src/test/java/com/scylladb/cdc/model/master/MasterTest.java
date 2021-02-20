package com.scylladb.cdc.model.master;

import com.google.common.collect.Lists;
import com.scylladb.cdc.cql.InMemoryMasterCQL;
import com.scylladb.cdc.model.*;
import com.scylladb.cdc.transport.MasterTransport;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.verification.VerificationWithTimeout;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.scylladb.cdc.model.master.MockGenerationMetadata.mockGenerationMetadata;
import static org.mockito.Mockito.*;

public class MasterTest {
    private static final long DEFAULT_VERIFY_TIMEOUT_MS = 1000;

    private static final List<GenerationMetadata> TEST_SET_ONE_GENERATION = Lists.newArrayList(
            MockGenerationMetadata.mockGenerationMetadata(5, Optional.empty(), 8)
    );

    private static final List<GenerationMetadata> TEST_SET_TWO_GENERATIONS = Lists.newArrayList(
            MockGenerationMetadata.mockGenerationMetadata(5, Optional.of(10L), 8),
            MockGenerationMetadata.mockGenerationMetadata(10, Optional.empty(), 8)
    );

    private static final Set<TableName> TEST_SET_SINGLE_TABLE = Collections.singleton(
            new TableName("ks", "test")
    );

    @Test
    public void testMasterConfiguresOneGeneration() throws InterruptedException {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MasterTransport masterTransport = mock(MasterTransport.class);
        when(masterTransport.getCurrentGenerationId()).thenReturn(Optional.empty());
        when(masterTransport.areTasksFullyConsumedUntil(any(), any())).thenReturn(false);
        InOrder configureWorkersOrder = inOrder(masterTransport);

        InMemoryMasterCQL masterCQL = spy(new InMemoryMasterCQL(TEST_SET_TWO_GENERATIONS));
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation...
            Map<TaskId, SortedSet<StreamId>> firstGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(0), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(firstGenerationConfigureWorkers));

            // ...and it checked multiple times if it should move to the
            //    next generation...
            verify(masterTransport, defaultTimeout().atLeast(5))
                    .areTasksFullyConsumedUntil(eq(firstGenerationConfigureWorkers.keySet()), any());

            // ...but ultimately it didn't go to the next generation.
            configureWorkersOrder.verify(masterTransport, never()).configureWorkers(any());
        }
    }

    @Test
    public void testMasterConfiguresTwoGenerations() throws InterruptedException {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MasterTransport masterTransport = mock(MasterTransport.class);
        when(masterTransport.getCurrentGenerationId()).thenReturn(Optional.empty());
        when(masterTransport.areTasksFullyConsumedUntil(any(), any())).thenReturn(false);
        InOrder configureWorkersOrder = inOrder(masterTransport);

        InMemoryMasterCQL masterCQL = spy(new InMemoryMasterCQL(TEST_SET_TWO_GENERATIONS));
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation...
            Map<TaskId, SortedSet<StreamId>> firstGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(0), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(firstGenerationConfigureWorkers));

            // ...and it checked multiple times if it should move to the
            //    next generation, but did not do it...
            verify(masterTransport, defaultTimeout().atLeast(5))
                    .areTasksFullyConsumedUntil(eq(firstGenerationConfigureWorkers.keySet()), any());
            configureWorkersOrder.verify(masterTransport, never()).configureWorkers(any());

            // ...until the first generation was fully consumed and
            //    only then the move occurred.
            when(masterTransport.areTasksFullyConsumedUntil(eq(firstGenerationConfigureWorkers.keySet()), any()))
                    .thenReturn(true);
            Map<TaskId, SortedSet<StreamId>> secondGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(secondGenerationConfigureWorkers));
        }
    }

    @Test
    public void testMasterDiscoversNewGeneration() throws InterruptedException {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MasterTransport masterTransport = mock(MasterTransport.class);
        when(masterTransport.getCurrentGenerationId()).thenReturn(Optional.empty());
        when(masterTransport.areTasksFullyConsumedUntil(any(), any())).thenReturn(false);
        InOrder configureWorkersOrder = inOrder(masterTransport);

        // Start with only a single generation visible.
        InMemoryMasterCQL masterCQL = spy(new InMemoryMasterCQL(TEST_SET_ONE_GENERATION));
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation...
            Map<TaskId, SortedSet<StreamId>> firstGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_ONE_GENERATION.get(0), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(firstGenerationConfigureWorkers));

            // ...and it checked multiple times if the current generation closed...
            verify(masterCQL, defaultTimeout().atLeast(5))
                    .fetchGenerationEnd(eq(TEST_SET_ONE_GENERATION.get(0).getId()));

            // ...but because it was not closed, it did not check if tasks were fully consumed.
            verify(masterTransport, never()).areTasksFullyConsumedUntil(any(), any());

            // New generation appears...
            masterCQL.setGenerationMetadatas(TEST_SET_TWO_GENERATIONS);

            // ...now we check if we should move to the next generation - not yet!...
            verify(masterTransport, defaultTimeout().atLeast(5))
                    .areTasksFullyConsumedUntil(eq(firstGenerationConfigureWorkers.keySet()), any());
            configureWorkersOrder.verify(masterTransport, never()).configureWorkers(any());

            // ...make current generation fully consumed...
            when(masterTransport.areTasksFullyConsumedUntil(eq(firstGenerationConfigureWorkers.keySet()), any()))
                    .thenReturn(true);

            // ...and observe moving to the next one.
            Map<TaskId, SortedSet<StreamId>> secondGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(secondGenerationConfigureWorkers));
        }
    }

    @Test
    public void testMasterResumesFromCurrentGeneration() throws InterruptedException {
        // MasterTransport with specified current generation
        // and never finishing any generation.
        MasterTransport masterTransport = mock(MasterTransport.class);
        when(masterTransport.getCurrentGenerationId()).thenReturn(Optional.of(TEST_SET_TWO_GENERATIONS.get(1).getId()));
        when(masterTransport.areTasksFullyConsumedUntil(any(), any())).thenReturn(false);
        InOrder configureWorkersOrder = inOrder(masterTransport);

        InMemoryMasterCQL masterCQL = spy(new InMemoryMasterCQL(TEST_SET_TWO_GENERATIONS));
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the second generation.
            Map<TaskId, SortedSet<StreamId>> secondGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(secondGenerationConfigureWorkers));
        }
    }

    @Test
    public void testMasterWaitsForFirstGeneration() throws InterruptedException {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MasterTransport masterTransport = mock(MasterTransport.class);
        when(masterTransport.getCurrentGenerationId()).thenReturn(Optional.empty());
        when(masterTransport.areTasksFullyConsumedUntil(any(), any())).thenReturn(false);
        InOrder configureWorkersOrder = inOrder(masterTransport);

        InMemoryMasterCQL masterCQL = spy(new InMemoryMasterCQL());
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the master tries to find the first generation.
            verify(masterCQL, defaultTimeout().atLeast(5)).fetchFirstGenerationId();
            configureWorkersOrder.verify(masterTransport, never()).configureWorkers(any());

            // First generation appears...
            masterCQL.setGenerationMetadatas(TEST_SET_ONE_GENERATION);

            // ...and then the transport receives it.
            Map<TaskId, SortedSet<StreamId>> firstGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_ONE_GENERATION.get(0), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(firstGenerationConfigureWorkers));
        }
    }

    @Test
    public void testMasterResilientToCQLExceptions() throws InterruptedException {
        // MasterTransport without an initial generation
        // (starting from beginning) and never finishing
        // any generation.
        MasterTransport masterTransport = mock(MasterTransport.class);
        when(masterTransport.getCurrentGenerationId()).thenReturn(Optional.empty());
        when(masterTransport.areTasksFullyConsumedUntil(any(), any())).thenReturn(false);
        InOrder configureWorkersOrder = inOrder(masterTransport);

        InMemoryMasterCQL masterCQL = spy(new InMemoryMasterCQL(TEST_SET_TWO_GENERATIONS));
        Set<TableName> tableNames = TEST_SET_SINGLE_TABLE;

        try (MasterThread masterThread = new MasterThread(masterTransport, masterCQL, tableNames)) {
            // Verify that the transport received the first generation.
            Map<TaskId, SortedSet<StreamId>> firstGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(0), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(firstGenerationConfigureWorkers));

            // Make the current generation fully consumed.
            when(masterTransport.areTasksFullyConsumedUntil(eq(firstGenerationConfigureWorkers.keySet()), any())).thenReturn(true);

            // New generation appears, but we simulate constant CQL failure...
            CompletableFuture<GenerationMetadata> fetchGenerationMetadataFail = new CompletableFuture<>();
            fetchGenerationMetadataFail.completeExceptionally(new IllegalAccessError("Injected fetchGenerationMetadata fail."));
            when(masterCQL.fetchGenerationMetadata(any())).thenReturn(fetchGenerationMetadataFail);

            CompletableFuture<Optional<Timestamp>> fetchGenerationEndFail = new CompletableFuture<>();
            fetchGenerationEndFail.completeExceptionally(new IllegalAccessError("Injected fetchGenerationEnd fail."));
            when(masterCQL.fetchGenerationEnd(any())).thenReturn(fetchGenerationEndFail);

            masterCQL.setGenerationMetadatas(TEST_SET_TWO_GENERATIONS);

            // ...CQL queries are performed, but they fail and no new generation is discovered...
            clearInvocations(masterCQL);
            verify(masterCQL, defaultTimeout().atLeast(5))
                    .fetchGenerationMetadata(eq(TEST_SET_TWO_GENERATIONS.get(0).getId()));
            configureWorkersOrder.verify(masterTransport, never()).configureWorkers(any());

            // ...the CQL is back working...
            when(masterCQL.fetchGenerationMetadata(any())).thenCallRealMethod();
            when(masterCQL.fetchGenerationEnd(any())).thenCallRealMethod();

            // ...and observe moving to the next generation.
            Map<TaskId, SortedSet<StreamId>> secondGenerationConfigureWorkers =
                    expectedConfigureWorkers(TEST_SET_TWO_GENERATIONS.get(1), tableNames);
            configureWorkersOrder.verify(masterTransport, defaultTimeout().times(1))
                    .configureWorkers(eq(secondGenerationConfigureWorkers));
        }
    }

    private VerificationWithTimeout defaultTimeout() {
        return timeout(DEFAULT_VERIFY_TIMEOUT_MS);
    }

    private Map<TaskId, SortedSet<StreamId>> expectedConfigureWorkers(GenerationMetadata generationMetadata, Set<TableName> tableNames) {
        Map<TaskId, SortedSet<StreamId>> result = new HashMap<>();
        List<VNodeId> vnodes = generationMetadata.getStreams().stream()
                .map(StreamId::getVNodeId).distinct().collect(Collectors.toList());
        // FIXME - quadratic complexity (|vnodes|^2)
        for (TableName tableName : tableNames) {
            for (VNodeId vnodeId : vnodes) {
                TaskId taskId = new TaskId(generationMetadata.getId(), vnodeId, tableName);
                SortedSet<StreamId> streamIds = generationMetadata.getStreams().stream()
                        .filter(s -> s.getVNodeId().equals(vnodeId)).collect(Collectors.toCollection(TreeSet::new));
                result.put(taskId, streamIds);
            }
        }
        return result;
    }
}
