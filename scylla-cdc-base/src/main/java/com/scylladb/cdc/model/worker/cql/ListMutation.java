package com.scylladb.cdc.model.worker.cql;

import com.google.common.base.Preconditions;

import java.util.*;

public class ListMutation {
    public enum MutationType {
        ADD,
        DELETE,
        OVERWRITE
    }

    private Map<Field, Field> elements;
    private Set<Field> removedElements;
    private List<Field> frozenOverwrite;
    private MutationType mutationType;

    public ListMutation(Map<Field, Field> elements, Set<Field> removedElements, List<Field> frozenOverwrite, MutationType mutationType) {
        this.elements = elements;
        this.removedElements = removedElements;
        this.frozenOverwrite = frozenOverwrite;
        this.mutationType = Preconditions.checkNotNull(mutationType);
    }

    public Collection<Field> getAddedElements() {
        // exception gdy mutationType zly
        return elements.values();
    }

    public Map<Field, Field> getAddedElementsAndTimestamps() {
        return Collections.unmodifiableMap(elements);
    }

    public Collection<Field> getOverwriteElements() {
        if (frozenOverwrite != null) {
            return frozenOverwrite;
        }
        return elements.values();
    }

    public Map<Field, Field> getOverwriteElementsAndTimestamps() {
        return Collections.unmodifiableMap(elements);
    }

    public Set<Field> getDeletedTimestamps() {
        return Collections.unmodifiableSet(removedElements);
    }

    // FIXME - maybe add a getDeletedElements() that
    // also involves the preimage

    public MutationType getMutationType() {
        return mutationType;
    }
}
