package com.scylladb.cdc.model.worker.cql;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MapMutation {
    public enum MutationType {
        ADD,
        DELETE,
        OVERWRITE
    }

    private Map<Field, Field> elements;
    private Set<Field> removedKeys;
    private MutationType mutationType;

    public MapMutation(Map<Field, Field> elements, Set<Field> removedKeys, MutationType mutationType) {
        this.elements = elements;
        this.removedKeys = removedKeys;
        this.mutationType = Preconditions.checkNotNull(mutationType);
    }

    public Map<Field, Field> getAddedElements() {
        // exception gdy mutationType zly
        return Collections.unmodifiableMap(elements);
    }

    public Map<Field, Field> getOverwriteElements() {
        return Collections.unmodifiableMap(elements);
    }

    public Set<Field> getDeletedElements() {
        return Collections.unmodifiableSet(removedKeys);
    }

    public MutationType getMutationType() {
        return mutationType;
    }
}
