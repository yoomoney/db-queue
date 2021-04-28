package ru.yoomoney.tech.dbqueue.config;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Properties for connection to a database shard.
 *
 * @param <DatabaseAccessLayerT> type of database access layer.
 * @author Oleg Kandaurov
 * @since 13.08.2018
 */
public class QueueShard<DatabaseAccessLayerT extends DatabaseAccessLayer> {

    @Nonnull
    private final QueueShardId shardId;
    @Nonnull
    private final DatabaseAccessLayerT databaseAccessLayer;

    /**
     * Constructor
     *
     * @param shardId             Shard identifier.
     * @param databaseAccessLayer database access layer.
     */
    public QueueShard(@Nonnull QueueShardId shardId,
                      @Nonnull DatabaseAccessLayerT databaseAccessLayer) {
        this.shardId = requireNonNull(shardId);
        this.databaseAccessLayer = requireNonNull(databaseAccessLayer);
    }

    /**
     * Get shard identifier.
     *
     * @return Shard identifier.
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Get database access layer.
     *
     * @return database access layer.
     */
    @Nonnull
    public DatabaseAccessLayerT getDatabaseAccessLayer() {
        return databaseAccessLayer;
    }
}
