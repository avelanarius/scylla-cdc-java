package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.cql.Cell;

import java.util.Iterator;
import java.util.stream.Stream;

public class PartitionDeleteChange {
    private RawChange deltaRawChange;

    // FIXME - change to protected
    public PartitionDeleteChange(RawChange deltaRawChange) {
        this.deltaRawChange = Preconditions.checkNotNull(deltaRawChange);

        Preconditions.checkArgument(deltaRawChange.getOperationType() == RawChange.OperationType.PARTITION_DELETE);
    }

    public ChangeId getId() {
        return deltaRawChange.getId();
    }

    public ChangeSchema getSchema() {
        // FIXME: Think about it.
        return deltaRawChange.getSchema();
    }

    public Stream<Cell> getPartitionKeyStream() {
        return deltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.PARTITION_KEY));
    }

    public Iterator<Cell> getPartitionKeyIterator() {
        return getPartitionKeyStream().iterator();
    }

    public Stream<Cell> getClusteringKeyStream() {
        return deltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY));
    }

    // FIXME - should there be getCell?
    public Cell getCell(String columnName) {
        // FIXME - forbid getting non-pk cells
        return deltaRawChange.getCell(columnName);
    }

    public RawChange getDeltaRawChange() {
        return deltaRawChange;
    }
}
