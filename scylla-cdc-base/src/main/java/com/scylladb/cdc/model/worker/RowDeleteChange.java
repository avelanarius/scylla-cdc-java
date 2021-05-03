package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.cql.Cell;

import java.util.Iterator;
import java.util.stream.Stream;

public class RowDeleteChange {
    private RawChange preimageRawChange;
    private RawChange deltaRawChange;

    // FIXME - change to protected
    public RowDeleteChange(RawChange preimageRawChange, RawChange deltaRawChange) {
        this.preimageRawChange = preimageRawChange;
        this.deltaRawChange = Preconditions.checkNotNull(deltaRawChange);

        Preconditions.checkArgument(deltaRawChange.getOperationType() == RawChange.OperationType.ROW_DELETE);
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

    public Iterator<Cell> getClusteringKeyIterator() {
        return getClusteringKeyStream().iterator();
    }

    public Stream<Cell> getPrimaryKeyStream() {
        return deltaRawChange.dataStream()
                .filter(c -> c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.PARTITION_KEY)
                        || c.getColumnDefinition().getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY));
    }

    public Iterator<Cell> getPrimaryKeyIterator() {
        return getPrimaryKeyStream().iterator();
    }

    public Cell getCell(String columnName) {
        // FIXME - disallow getting strange cells
        return deltaRawChange.getCell(columnName);
    }

    private boolean isCellWorthy(Cell cell) {
        ChangeSchema.ColumnDefinition columnDefinition = cell.getColumnDefinition();
        if (columnDefinition.getBaseTableColumnType().equals(ChangeSchema.ColumnType.PARTITION_KEY)
                || columnDefinition.getBaseTableColumnType().equals(ChangeSchema.ColumnType.CLUSTERING_KEY)) {
            return true;
        }
        ChangeSchema.ColumnDefinition deletedColumn = cell.getColumnDefinition().getDeletedColumn(getSchema());
        Boolean isDeleted = deltaRawChange.getCell(deletedColumn).getBoolean();
        if (cell.isNull() && (isDeleted == null || !isDeleted)) {
            return false;
        }
        return true;
    }

    public boolean hasPreimage() {
        return preimageRawChange != null;
    }

    public Stream<Cell> getPreimageCellsStream() {
        // fallback for pre-cdc$deleted_ fix
        return preimageRawChange.dataStream().filter(this::isCellWorthy);
    }

    public Iterator<Cell> getPreimageCellsIterator() {
        return getPreimageCellsStream().iterator();
    }

    public Cell getPreimageCell(String columnName) {
        Cell cell = preimageRawChange.getCell(columnName);
        if (isCellWorthy(cell)) {
            return cell;
        } else {
            return null;
        }
    }

    public RawChange getPreimageRawChange() {
        return preimageRawChange;
    }

    public RawChange getDeltaRawChange() {
        return deltaRawChange;
    }
}
