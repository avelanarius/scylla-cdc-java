package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class ScyllaOffsetContext implements OffsetContext {

    private final Map<TaskId, SourceInfo> sourceInfos;
    private final TransactionContext transactionContext;

    public ScyllaOffsetContext(Map<TaskId, SourceInfo> sourceInfos, TransactionContext transactionContext) {
        this.sourceInfos = sourceInfos;
        this.transactionContext = transactionContext;
    }

    public TaskStateOffsetContext taskStateOffsetContext(TaskId taskId) {
        return new TaskStateOffsetContext(this, sourceInfos.get(taskId));
    }

    @Override
    public Map<String, ?> getPartition() {
        // See TaskStateOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ?> getOffset() {
        // See TaskStateOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public Schema getSourceInfoSchema() {
        // See TaskStateOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public Struct getSourceInfo() {
        // See TaskStateOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSnapshotRunning() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markLastSnapshotRecord() {

    }

    @Override
    public void preSnapshotStart() {

    }

    @Override
    public void preSnapshotCompletion() {

    }

    @Override
    public void postSnapshotCompletion() {

    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        // Not used by the Scylla CDC Source Connector.
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }
}
