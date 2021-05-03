package com.scylladb.cdc.model.worker.cql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class ProcessedUpdateCell {
    private final RawChange rawChange;
    private final Cell cell;
    private final Cell deletedCell;
    private final Cell deletedElementsCell;

    public ProcessedUpdateCell(RawChange rawChange, Cell cell, Cell deletedCell, Cell deletedElementsCell) {
        this.rawChange = Preconditions.checkNotNull(rawChange);
        this.cell = Preconditions.checkNotNull(cell);
        this.deletedCell = deletedCell;
        this.deletedElementsCell = deletedElementsCell;
    }

    public Object getAsObject() {
        // does it make sense - what should it return for
        // mutation of non-frozen collection?
        return cell.getAsObject();
    }

    public ChangeSchema.DataType getDataType() {
        return cell.getDataType();
    }

    public boolean isNull() {
        // FIXME - wrong for non-frozen collections?
        return cell.isNull();
    }

    public ByteBuffer getBytes() {
        return cell.getBytes();
    }

    public String getString() {
        return cell.getString();
    }

    public BigInteger getVarint() {
        return cell.getVarint();
    }

    public BigDecimal getDecimal() {
        return cell.getDecimal();
    }

    public UUID getUUID() {
        return cell.getUUID();
    }

    public InetAddress getInet() {
        return cell.getInet();
    }

    public Float getFloat() {
        return cell.getFloat();
    }

    public Double getDouble() {
        return cell.getDouble();
    }

    public Long getLong() {
        return cell.getLong();
    }

    public Integer getInt() {
        return cell.getInt();
    }

    public Short getShort() {
        return cell.getShort();
    }

    public Byte getByte() {
        return cell.getByte();
    }

    public Boolean getBoolean() {
        return cell.getBoolean();
    }

    public List<Field> getTuple() {
        return cell.getTuple();
    }

    public Date getTimestamp() {
        return cell.getTimestamp();
    }

    public Long getTime() {
        return cell.getTime();
    }

    public CqlDuration getDuration() {
        return cell.getDuration();
    }

    public CqlDate getDate() {
        return cell.getDate();
    }

    // Collection types:

    // Pytanie czy jak mamy post-image, to czy powinnismy zwracac MapMutation,
    // czy post-image. Wydaje sie, ze lepiej MapMutation, bo consistency.

    public MapMutation getMap() {
        boolean isFrozen = cell.getColumnDefinition().getBaseTableDataType().isFrozen();
        if (isFrozen) {
            return new MapMutation(cell.getMap(), null, MapMutation.MutationType.OVERWRITE);
        } else {
            if (!cell.isNull() && deletedCell.getBoolean() == null) {
                return new MapMutation(cell.getMap(), null, MapMutation.MutationType.ADD);
            } else if (!cell.isNull() && deletedCell.getBoolean() != null && deletedCell.getBoolean()) {
                return new MapMutation(cell.getMap(), null, MapMutation.MutationType.OVERWRITE);
            } else {
                return new MapMutation(null, deletedElementsCell.getSet(), MapMutation.MutationType.DELETE);
            }
        }
    }

    public SetMutation getSet() {
        boolean isFrozen = cell.getColumnDefinition().getBaseTableDataType().isFrozen();
        if (isFrozen) {
            return new SetMutation(cell.getSet(), SetMutation.MutationType.OVERWRITE);
        } else {
            if (!cell.isNull() && deletedCell.getBoolean() == null) {
                return new SetMutation(cell.getSet(), SetMutation.MutationType.ADD);
            } else if (!cell.isNull() && deletedCell.getBoolean() != null && deletedCell.getBoolean()) {
                return new SetMutation(cell.getSet(), SetMutation.MutationType.OVERWRITE);
            } else {
                return new SetMutation(deletedElementsCell.getSet(), SetMutation.MutationType.DELETE);
            }
        }
    }

    public ListMutation getList() {
        boolean isFrozen = cell.getColumnDefinition().getBaseTableDataType().isFrozen();
        if (isFrozen) {
            return new ListMutation(null, null, cell.getList(), ListMutation.MutationType.OVERWRITE);
        } else {
            if (!cell.isNull() && deletedCell.getBoolean() == null) {
                return new ListMutation(cell.getMap(), null, null, ListMutation.MutationType.ADD);
            } else if (!cell.isNull() && deletedCell.getBoolean() != null && deletedCell.getBoolean()) {
                return new ListMutation(cell.getMap(), null, null, ListMutation.MutationType.OVERWRITE);
            } else {
                return new ListMutation(null, deletedElementsCell.getSet(), null, ListMutation.MutationType.DELETE);
            }
        }
    }

    public UdtMutation getUDT() {
        boolean isFrozen = cell.getColumnDefinition().getBaseTableDataType().isFrozen();
        ChangeSchema.DataType.UdtType udtType = cell.getColumnDefinition().getCdcLogDataType().getUdtType();
        if (isFrozen) {
            return new UdtMutation(cell.getUDT(), null, udtType, UdtMutation.MutationType.OVERWRITE);
        } else {
            if (!cell.isNull() && deletedCell.getBoolean() == null) {
                return new UdtMutation(cell.getUDT(), deletedElementsCell.getSet(), udtType, UdtMutation.MutationType.UPDATE);
            } else {
                return new UdtMutation(cell.getUDT(), deletedElementsCell.getSet(), udtType, UdtMutation.MutationType.OVERWRITE);
            }
        }
    }

    // tostring, equls, hashcode itp
}
