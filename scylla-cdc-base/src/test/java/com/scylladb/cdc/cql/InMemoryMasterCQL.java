package com.scylladb.cdc.cql;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class InMemoryMasterCQL implements MasterCQL {
    private volatile List<GenerationMetadata> generationMetadatas;

    public InMemoryMasterCQL() {
        this(Collections.emptyList());
    }

    public InMemoryMasterCQL(List<GenerationMetadata> generationMetadatas) {
        this.generationMetadatas = Preconditions.checkNotNull(generationMetadatas);
    }

    public void setGenerationMetadatas(List<GenerationMetadata> generationMetadatas) {
        this.generationMetadatas = generationMetadatas;
    }

    @Override
    public CompletableFuture<Optional<GenerationId>> fetchFirstGenerationId() {
        Optional<GenerationId> result = generationMetadatas.stream().map(GenerationMetadata::getId).min(Comparator.naturalOrder());
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<GenerationMetadata> fetchGenerationMetadata(GenerationId id) {
        Optional<GenerationMetadata> generationMetadata = generationMetadatas.stream().filter(g -> g.getId().equals(id)).findFirst();
        return generationMetadata.map(CompletableFuture::completedFuture).orElseGet(() -> {
            CompletableFuture<GenerationMetadata> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new IllegalArgumentException(
                    String.format("Could not fetch generation metadata with id: %s", id)));
            return failedFuture;
        });
    }

    @Override
    public CompletableFuture<Optional<Timestamp>> fetchGenerationEnd(GenerationId id) {
        return fetchGenerationMetadata(id).thenApply(GenerationMetadata::getEnd);
    }
}
