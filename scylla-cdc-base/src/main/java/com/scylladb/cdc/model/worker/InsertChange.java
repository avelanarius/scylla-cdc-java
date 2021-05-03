package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.cql.Cell;

import java.util.Iterator;
import java.util.stream.Stream;

public class InsertChange {
    private RawChange preimageRawChange;
    private RawChange deltaRawChange;
    private RawChange postimageRawChange;

    // FIXME - change to protected
    public InsertChange(RawChange preimageRawChange, RawChange deltaRawChange, RawChange postimageRawChange) {
        this.preimageRawChange = preimageRawChange;
        this.deltaRawChange = Preconditions.checkNotNull(deltaRawChange);
        this.postimageRawChange = postimageRawChange;

        Preconditions.checkArgument(deltaRawChange.getOperationType() == RawChange.OperationType.ROW_INSERT);
    }

    public ChangeId getId() {
        return deltaRawChange.getId();
    }

    public Long getTTL() {
        return deltaRawChange.getTTL();
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
        // FIXME - maybe a wrapper around Cell - ProcessedCell
        // and then ProcessedCellWithFrozenUnfrozenDistinction
        Cell cell = deltaRawChange.getCell(columnName);
        // FIXME - forbid getting "strange" cells
        if (isCellWorthy(cell)) {
            return cell;
        } else {
            return null;
        }
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

    public Stream<Cell> getCellsStream() {
        return deltaRawChange.dataStream().filter(this::isCellWorthy);
    }

    public Iterator<Cell> getCellsIterator() {
        return getCellsStream().iterator();
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

    public boolean hasPostimage() {
        return postimageRawChange != null;
    }

    public Stream<Cell> getPostimageCellsStream() {
        // fixme - describe why no need to filter!
        return postimageRawChange.dataStream();
    }

    public Iterator<Cell> getPostimageCellsIterator() {
        return getPostimageCellsStream().iterator();
    }

    public Cell getPostimageCell(String columnName) {
        return postimageRawChange.getCell(columnName);
    }

    public RawChange getPreimageRawChange() {
        return preimageRawChange;
    }

    public RawChange getDeltaRawChange() {
        return deltaRawChange;
    }

    public RawChange getPostimageRawChange() {
        return postimageRawChange;
    }
}
