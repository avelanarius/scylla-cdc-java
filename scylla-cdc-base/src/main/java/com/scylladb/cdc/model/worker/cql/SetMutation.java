package com.scylladb.cdc.model.worker.cql;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Set;

public class SetMutation {
    public enum MutationType {
        ADD,
        DELETE,
        OVERWRITE
    }

    private Set<Field> elements;
    private MutationType mutationType;

    public SetMutation(Set<Field> elements, MutationType mutationType) {
        this.elements = elements;
        this.mutationType = Preconditions.checkNotNull(mutationType);
    }

    // 3 gettery moze? getAddedElements, getDeletedElements itp
    // consistent with other *Mutation classes

    public Set<Field> getElements() {
        // If null, maybe it should return empty set?
        // Scylla interprets NULL as empty set?
        return Collections.unmodifiableSet(elements);
    }

    public MutationType getMutationType() {
        return mutationType;
    }

    // tostring, equals, hashcode itp
}
