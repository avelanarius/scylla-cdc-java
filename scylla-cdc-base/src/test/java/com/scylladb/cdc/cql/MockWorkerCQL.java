package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.MockRawChange;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MockWorkerCQL implements WorkerCQL {
    private volatile Map<TableName, Optional<Long>> tablesTTL = new HashMap<>();
    private volatile List<RawChange> rawChanges = Collections.emptyList();

    private final Set<Task> createReaderInvocations = ConcurrentHashMap.newKeySet();
    private final Set<Task> finishedReaders = ConcurrentHashMap.newKeySet();

    private final Multiset<RawChange> injectedRawChangeFailures = ConcurrentHashMultiset.create();
    private final Multiset<RawChange> injectedRawChangeConstantFailures = ConcurrentHashMultiset.create();
    private final AtomicInteger failureCount = new AtomicInteger(0);

    class MockReaderCQL implements WorkerCQL.Reader {
        private final Task task;
        private final Iterator<RawChange> rawChangeIterator;

        public MockReaderCQL(Task task, Iterator<RawChange> rawChangeIterator) {
            this.task = task;
            this.rawChangeIterator = Preconditions.checkNotNull(rawChangeIterator);
        }

        @Override
        public CompletableFuture<Optional<RawChange>> nextChange() {
            if (rawChangeIterator.hasNext()) {
                RawChange readChange = rawChangeIterator.next();
                if (injectedRawChangeFailures.remove(readChange)) {
                    failureCount.incrementAndGet();
                    CompletableFuture<Optional<RawChange>> injectedFailure = new CompletableFuture<>();
                    injectedFailure.completeExceptionally(new RuntimeException("Injected exception in nextChange()"));
                    return injectedFailure;
                }
                if (injectedRawChangeConstantFailures.contains(readChange)) {
                    failureCount.incrementAndGet();
                    CompletableFuture<Optional<RawChange>> injectedFailure = new CompletableFuture<>();
                    injectedFailure.completeExceptionally(new RuntimeException("Injected constant exception in nextChange()"));
                    return injectedFailure;
                }
                return CompletableFuture.completedFuture(Optional.of(readChange));
            }
            finishedReaders.add(task);
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }

    @Override
    public void prepare(Set<TableName> tables) {
        // No-op
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
        createReaderInvocations.add(task);

        long taskStartMs = task.state.getWindowStartTimestamp().toDate().getTime();
        long taskEndMs = task.state.getWindowEndTimestamp().toDate().getTime();
        Optional<ChangeId> lastConsumedChangeId = task.state.getLastConsumedChangeId();

        List<RawChange> collectedChanges = rawChanges.stream().filter(change -> {
            // FIXME: Also check table name.

            if (task.streams.stream().noneMatch(s -> s.equals(change.getId().getStreamId()))) {
                return false;
            }

            long changeTimestampMs = change.getId().getChangeTime().getDate().getTime();
            if (changeTimestampMs < taskStartMs || changeTimestampMs >= taskEndMs) {
                return false;
            }

            if (lastConsumedChangeId.isPresent()) {
                return change.getId().compareTo(lastConsumedChangeId.get()) > 0;
            }

            return true;
        }).collect(Collectors.toList());

        return CompletableFuture.completedFuture(new MockReaderCQL(task, collectedChanges.iterator()));
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
        Optional<Long> ttl = tablesTTL.getOrDefault(tableName, Optional.empty());
        return CompletableFuture.completedFuture(ttl);
    }

    public void setRawChanges(List<MockRawChange> rawChanges) {
        this.rawChanges = rawChanges.stream().sorted(MockRawChange::compareTo).collect(Collectors.toList());
    }

    public void setTablesTTL(Map<TableName, Optional<Long>> tablesTTL) {
        this.tablesTTL = tablesTTL;
    }

    public boolean wasCreateReaderInvoked(Task task) {
        return createReaderInvocations.contains(task);
    }

    public boolean isReaderFinished(Task task) {
        return finishedReaders.contains(task);
    }

    public void injectRawChangeFailure(RawChange change) {
        injectedRawChangeFailures.add(change);
    }

    public void injectConstantRawChangeFailure(RawChange change) {
        injectedRawChangeConstantFailures.add(change);
    }

    public void deinjectConstantRawChangeFailure(RawChange change) {
        injectedRawChangeConstantFailures.remove(change);
    }

    public int getFailureCount() {
        return failureCount.get();
    }
}
