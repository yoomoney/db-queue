package ru.yandex.money.common.dbqueue.api;


/**
 * Интерфейс, предоставляющий правила помещения и обработки задач на шардах БД.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 29.07.2017
 */
public interface QueueShardRouter<T> extends QueueProducer.ProducerShardRouter<T>, QueueConsumer.ConsumerShardsProvider {

}
