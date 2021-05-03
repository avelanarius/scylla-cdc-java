package com.scylladb.cdc.model.worker.cql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class UdtMutation {
    public enum MutationType {
        UPDATE,
        OVERWRITE
    }

    private Map<String, Field> fields;
    private Set<Field> deletedElements;
    private ChangeSchema.DataType.UdtType udtType;
    private MutationType mutationType;

    public UdtMutation(Map<String, Field> fields, Set<Field> deletedElements,
                       ChangeSchema.DataType.UdtType udtType, MutationType mutationType) {
        this.fields = fields;
        this.deletedElements = deletedElements;
        this.udtType = Preconditions.checkNotNull(udtType);
        this.mutationType = Preconditions.checkNotNull(mutationType);
    }

    // = NULL w UDT nie rozumiem jako delete!
    public Map<String, Field> getUpdatedFields() {
        ImmutableMap.Builder<String, Field> result = new ImmutableMap.Builder<>();
        int index = 0;
        for (Map.Entry<String, Field> field : fields.entrySet()) {
            if (field.getValue().getAsObject() == null && !deletedElementsContainsIndex(index)) {
                // NULL in UDT, but it is not a part of cdc$deleted_elements_
                // Therefore, this UDT field was not modified.
                index++;
             } else {
                index++;
                result.put(field.getKey(), field.getValue());
            }
        }
        return result.build();
    }

    private boolean deletedElementsContainsIndex(int index) {
        // FIXME: performance
        if (deletedElements == null) {
            return false;
        }
        return deletedElements.stream().mapToInt(Field::getShort).anyMatch(f -> f == index);
    }

    public Map<String, Field> getOverwriteFields() {
        return Collections.unmodifiableMap(fields);
    }
    public MutationType getMutationType() {
        return mutationType;
    }
}
