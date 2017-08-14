package ru.yandex.money.common.dbqueue.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.util.Objects;

/**
 * Управление и получение информации о задачах по полю actor.
 * <p>
 * ВНИМАНИЕ: Для использования этой функциональности необходимо добавить
 * составной индекс по полям (actor, queue_name)
 * <p>
 * CREATE INDEX actor_name_idx ON tasks (actor, queue_name)
 *
 * @author Oleg Kandaurov
 * @since 11.07.2017
 */
public class QueueActorDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionOperations transactionTemplate;

    /**
     * Конструктор
     *
     * @param jdbcTemplate        spring jdbc template
     * @param transactionTemplate spring transaction template
     */
    public QueueActorDao(JdbcOperations jdbcTemplate, TransactionOperations transactionTemplate) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * Удалить задачи в очереди по заданному actor.
     * <p>
     * Может применяться в том случае, когда стало известно, что поставленная задача уже не актуальна.
     * Удаление позволит уменьшить количество задач в очереди, что приведет к уменьшению затрат на выборку,
     * а также будет видно количество актуальных задач в очереди.
     * <p>
     * Альтернативный подход заключается в том, чтобы при постановке задачи, сохранять её идентификатор (sequence id)
     * и производить удаление по заданному идентификатору. Подобный способ решения является более низкоуровневым
     * требует отдельно хранить идентификаторы задач.
     * Однако такой подход является более производительным поскольку исключает создание дополнительного индекса.
     *
     * @param location местоположение очереди
     * @param actor    бизнесовый идентификатор
     * @return признак, что хотя бы одна задача была удалена
     */
    public boolean deleteTasksByActor(@Nonnull QueueLocation location, @Nonnull String actor) {
        Objects.requireNonNull(location);
        Objects.requireNonNull(actor);
        int updatedRows = jdbcTemplate.update(
                String.format("DELETE FROM %s WHERE actor = :actor " +
                        "AND queue_name = :queue_name", location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("queue_name", location.getQueueName())
                        .addValue("actor", actor));
        return updatedRows != 0;
    }

    /**
     * Проверить, что в очереди есть задачи по заданному actor.
     * <p>
     * Может применяться, чтобы исключить повторную вставку задачи на обработку.
     * При подобном использовании не гарантирует, что задача не вставится дважды.
     * Для использования в подобном ключе, внешний код должен использовать блокировки.
     *
     * @param location местоположение очереди
     * @param actor    бизнесовый идентификатор
     * @return признак, что задачи найдены по actor
     */
    public boolean isTasksExist(@Nonnull QueueLocation location, @Nonnull String actor) {
        Objects.requireNonNull(location);
        Objects.requireNonNull(actor);
        return jdbcTemplate.query(
                String.format("SELECT 1 FROM %s WHERE actor = :actor " +
                        "AND queue_name = :queueName LIMIT 1", location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("queueName", location.getQueueName())
                        .addValue("actor", actor),
                ResultSet::next
        );
    }

    /**
     * Получить transaction template
     *
     * @return spring transaction template
     */
    public TransactionOperations getTransactionTemplate() {
        return transactionTemplate;
    }
}
