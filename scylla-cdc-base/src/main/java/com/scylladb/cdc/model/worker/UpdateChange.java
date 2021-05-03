package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.ProcessedUpdateCell;

import java.util.Iterator;
import java.util.stream.Stream;

public class UpdateChange {
    private RawChange preimageRawChange;
    private RawChange deltaRawChange;
    private RawChange postimageRawChange;

    // FIXME - change to protected
    public UpdateChange(RawChange preimageRawChange, RawChange deltaRawChange, RawChange postimageRawChange) {
        this.preimageRawChange = preimageRawChange;
        this.deltaRawChange = Preconditions.checkNotNull(deltaRawChange);
        this.postimageRawChange = postimageRawChange;

        Preconditions.checkArgument(deltaRawChange.getOperationType() == RawChange.OperationType.ROW_UPDATE);
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

    public ProcessedUpdateCell getCell(String columnName) {
        // FIXME - maybe a wrapper around Cell - ProcessedCell
        // and then ProcessedCellWithFrozenUnfrozenDistinction
        Cell cell = deltaRawChange.getCell(columnName);
        if (isCellWorthy(cell)) {
            return translateCell(cell);
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
        // THis is correct logic for non-frozen stufferino - look also at cdc$deleted_elements_v,
        // but FIXME - clean it up!
        ChangeSchema.ColumnDefinition deletedColumn = cell.getColumnDefinition().getDeletedColumn(getSchema());
        if (!columnDefinition.getBaseTableDataType().isFrozen()) {
            ChangeSchema.ColumnDefinition deletedElementsColumn = cell.getColumnDefinition().getDeletedElementsColumn(getSchema());
            Cell deletedElementsCell = deltaRawChange.getCell(deletedElementsColumn);
            if (!deletedElementsCell.isNull()) {
                return true;
            }
        }
        Boolean isDeleted = deltaRawChange.getCell(deletedColumn).getBoolean();
        if (cell.isNull() && (isDeleted == null || !isDeleted)) {
            return false;
        }
        return true;
    }

    private ProcessedUpdateCell translateCell(Cell cell) {
        // There is no need to translate pk, cks, but ProcessedUpdateCell is not compatible with the other Cell
        if (cell.getColumnDefinition().getBaseTableColumnType() != ChangeSchema.ColumnType.REGULAR) {
            return new ProcessedUpdateCell(deltaRawChange, cell, null, null);
        }

        Cell deletedCell = deltaRawChange.getCell(cell.getColumnDefinition().getDeletedColumn(getSchema()));

        ChangeSchema.DataType dataType = cell.getColumnDefinition().getBaseTableDataType();
        if (dataType.isFrozen()) {
            return new ProcessedUpdateCell(deltaRawChange, cell, deletedCell, null);
        } else {
            Cell deletedElementsCell = deltaRawChange.getCell(cell.getColumnDefinition().getDeletedElementsColumn(getSchema()));
            return new ProcessedUpdateCell(deltaRawChange, cell, deletedCell, deletedElementsCell);
        }
    }

    public Stream<ProcessedUpdateCell> getCellsStream() {
        return deltaRawChange.dataStream().filter(this::isCellWorthy).map(this::translateCell);
    }

    public Iterator<ProcessedUpdateCell> getCellsIterator() {
        return getCellsStream().iterator();
    }

    public boolean hasPreimage() {
        return preimageRawChange != null;
    }

    public Stream<Cell> getPreimageCellsStream() {
        // fallback for pre-cdc$deleted_ fix

        // FIXME:
        // If table has non-frozen lists,
        // then the preimage/postimage has map<timeuuid, X>
        // not list<X>. Therefore, we need a wrapper
        // around Cell that unpacks timeuuid

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
        // fixme - no need to filter!
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
