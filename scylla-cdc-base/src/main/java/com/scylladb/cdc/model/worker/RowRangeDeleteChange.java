package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.cql.Cell;

import java.util.Iterator;
import java.util.stream.Stream;

public class RowRangeDeleteChange {
    // FIXME - finals everywhere
    private final RawChange leftBoundDeltaRawChange;
    private final RawChange rightBoundDeltaRawChange;

    // FIXME - change to protected
    public RowRangeDeleteChange(RawChange leftBoundDeltaRawChange, RawChange rightBoundDeltaRawChange) {
        this.leftBoundDeltaRawChange = Preconditions.checkNotNull(leftBoundDeltaRawChange);
        this.rightBoundDeltaRawChange = Preconditions.checkNotNull(rightBoundDeltaRawChange);

        Preconditions.checkArgument(leftBoundDeltaRawChange.getOperationType() == RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND
            || leftBoundDeltaRawChange.getOperationType() == RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_LEFT_BOUND);

        Preconditions.checkArgument(rightBoundDeltaRawChange.getOperationType() == RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND
            || rightBoundDeltaRawChange.getOperationType() == RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND);
    }

    public ChangeId getId() {
        return leftBoundDeltaRawChange.getId();
    }

    public ChangeSchema getSchema() {
        // FIXME: Think about it.

        // FIXME: schema can get changed inbetween?
        // FIXME: schema should be consistent within a batch?
        return leftBoundDeltaRawChange.getSchema();
    }

    public Stream<Cell> getPartitionKeyStream() {
        return leftBoundDeltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.PARTITION_KEY));
    }

    public Iterator<Cell> getPartitionKeyIterator() {
        return getPartitionKeyStream().iterator();
    }

    public Stream<Cell> getLeftBoundClusteringKeyStream() {
        return leftBoundDeltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY));
    }

    public Iterator<Cell> getLeftBoundClusteringKeyIterator() {
        return getLeftBoundClusteringKeyStream().iterator();
    }

    public boolean isLeftBoundInclusive() {
        return leftBoundDeltaRawChange.getOperationType() == RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND;
    }

    public boolean isLeftBoundExclusive() {
        return !isLeftBoundInclusive();
    }

    public Stream<Cell> getRightBoundClusteringKeyStream() {
        return rightBoundDeltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY));
    }

    public Iterator<Cell> getRightBoundClusteringKeyIterator() {
        return getRightBoundClusteringKeyStream().iterator();
    }

    public boolean isRightBoundInclusive() {
        return rightBoundDeltaRawChange.getOperationType() == RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND;
    }

    public boolean isRightBoundExclusive() {
        return !isRightBoundInclusive();
    }

    public Stream<Cell> getLeftBoundPrimaryKeyStream() {
        return leftBoundDeltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.PARTITION_KEY)
                        || c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY));
    }

    public Iterator<Cell> getLeftBoundPrimaryKeyIterator() {
        return getLeftBoundPrimaryKeyStream().iterator();
    }

    public Stream<Cell> getRightBoundPrimaryKeyStream() {
        return rightBoundDeltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.PARTITION_KEY)
                        || c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY));
    }

    public Iterator<Cell> getRightBoundPrimaryKeyIterator() {
        return getRightBoundPrimaryKeyStream().iterator();
    }

    // FIXME: And here it doesnt make sense to make a getCell() - getRightBoundCell(), getLeftBoundCell()??

    // FIXME: Better ways to get left, right bound - mostly for a case where there are multiple cks
    // and to skip those which are null - maybe a Stream should end at null?

    public RawChange getLeftBoundDeltaRawChange() {
        return leftBoundDeltaRawChange;
    }

    public RawChange getRightBoundDeltaRawChange() {
        return rightBoundDeltaRawChange;
    }
}
