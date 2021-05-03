package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CQLConfiguration {
    private static final int DEFAULT_PORT = 9042;

    public enum ConsistencyLevel {
        ANY,
        ONE,
        TWO,
        THREE,
        QUORUM,
        ALL,
        LOCAL_QUORUM,
        EACH_QUORUM,
        SERIAL,
        LOCAL_SERIAL,
        LOCAL_ONE
    }

    public final List<InetSocketAddress> contactPoints;
    public final String user;
    public final String password;
    public final ConsistencyLevel consistencyLevel;

    private CQLConfiguration(List<InetSocketAddress> contactPoints,
                            String user, String password, ConsistencyLevel consistencyLevel) {
        this.contactPoints = Preconditions.checkNotNull(contactPoints);
        Preconditions.checkArgument(!contactPoints.isEmpty());

        this.user = user;
        this.password = password;
        this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
        // Either someone did not provide credentials
        // or provided user-password pair.
        Preconditions.checkArgument((this.user == null && this.password == null)
                || (this.user != null && this.password != null));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<InetSocketAddress> contactPoints = new ArrayList<>();
        private String user = null;
        private String password = null;
        // nocheckin, zly consistencylevel, QUORUM 1 node to zle wiesci!
        private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

        public Builder addContactPoint(InetSocketAddress contactPoint) {
            Preconditions.checkNotNull(contactPoint);
            contactPoints.add(contactPoint);
            return this;
        }

        public Builder addContactPoints(Collection<InetSocketAddress> addedContactPoints) {
            for (InetSocketAddress contactPoint : addedContactPoints) {
                this.addContactPoint(contactPoint);
            }
            return this;
        }

        public Builder addContactPoint(String host, int port) {
            Preconditions.checkNotNull(host);
            Preconditions.checkArgument(port > 0 && port < 65536);
            return addContactPoint(new InetSocketAddress(host, port));
        }

        public Builder addContactPoint(String host) {
            return addContactPoint(host, DEFAULT_PORT);
        }

        public Builder withCredentials(String user, String password) {
            this.user = Preconditions.checkNotNull(user);
            this.password = Preconditions.checkNotNull(password);
            return this;
        }

        public Builder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = Preconditions.checkNotNull(consistencyLevel);
            return this;
        }

        public CQLConfiguration build() {
            return new CQLConfiguration(contactPoints, user, password, consistencyLevel);
        }
    }
}