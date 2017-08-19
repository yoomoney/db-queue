[![Build Status](https://travis-ci.org/yandex-money/db-queue.svg?branch=master)](https://travis-ci.org/yandex-money/db-queue)
[![Code Coverage](https://codecov.io/gh/yandex-money/db-queue/branch/feature/add_badges/graph/badge.svg)](https://codecov.io/gh/yandex-money/db-queue)
[![Codebeat](https://codebeat.co/badges/ff7a4c21-72fb-446c-b245-ba739240fe49)](https://codebeat.co/projects/github-com-yandex-money-db-queue-master)
[![Codacy](https://api.codacy.com/project/badge/Grade/3a0e23fae44843c284540929d750b65c)](https://www.codacy.com/app/f0y/db-queue?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=yandex-money/db-queue&amp;utm_campaign=Badge_Grade)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Download](https://api.bintray.com/packages/yandex-money/maven/db-queue/images/download.svg)](https://bintray.com/yandex-money/maven/db-queue/_latestVersion)

# Описание

Библиотека предоставляет реализацию очередей поверх базы данных.

Высокоуровневая схема работы:
1) У клиента есть задача с данными, которые он хочет обработать позднее.
2) Клиент, сообщает Enqueuer'у, что необходимо поместить задачу в очередь на обработку.
3) Enqueuer, посредством ShardRouter, выбирает шард базы данных, на которую нужно поместить задачу.
4) Enqueuer, при помощи PayloadTransformer, преобразует данные задачи в строковое представление.
5) Enqueuer вставляет задачу в БД используя QueueDao.
6) ... в назначенное время задача выбирается из БД ...
7) Строковое предоставление данных задачи преобразуется в типизированное при помощи PayloadTransformer.
8) Задача поступает в Queue для последующей обработки клиентом.
9) После выполнения задачи, клиент сообщает с каким результатом она обработана.

Значимые возможности:
* Поддержка БД Postgresql, начиная с версии 9.5.
* At-least-once семантика доставки задач (exactly-once при определенных условиях).
* Хранение задач нескольких очередей, как вместе, так и в разных таблицах (QueueLocation).
* Хранение задач очереди на разных шардах БД (ShardRouter).
* Управление стратегией откладывания задачи при возникновении ошибки обработки (TaskRetryType).
* Получение событий цикла обработки задачи (TaskLifecycleListener, QueueThreadLifecycleListener).
* Типизированное api для работы с данными задачи (PayloadTransformer).
* Различные режимы обработки задач (ProcessingMode).


# Использование

## Подключение

```
<dependency>
  <groupId>ru.yandex.money.common</groupId>
  <artifactId>db-queue</artifactId>
  <version>0.0.1</version>
</dependency>
```

## Конфигурация 

* Создать таблицу для хранения задач.
```sql
CREATE TABLE tasks (
  id            BIGSERIAL PRIMARY KEY,
  queue_name    VARCHAR(128) NOT NULL,
  task          TEXT,
  create_time   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  process_time  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt       INTEGER                  DEFAULT 0,
  actor         VARCHAR(128),
  log_timestamp VARCHAR(128)
);
CREATE INDEX tasks_name_time_desc_idx
  ON tasks (queue_name, process_time, id DESC);
```
* Задать настройки очереди через объект QueueConfig.
  * Выбрать имя очереди.
  * Задать настройки betweenTaskTimeout и noTaskTimeout в QueueSettings.
* Следовать ручной или spring конфигурации.

### Ручная конфигурация

Ручная конфигурация может быть использована там, где нет спрингового контекста.
В ней описание происходит более явно, все классы immutable и больше гибкости.
Пример конфигурации - [example.ManualConfiguration](https://github.com/yandex-money/db-queue/blob/master/src/test/java/example/ManualConfiguration.java).

* Создать QueueDao для работы с шардами
* Реализовать интерфейс ShardRouter или использовать SingleShardRouter
* Реализовать интерфейс PayloadTransformer или использовать NoopPayloadTransformer
* Реализовать интерфейс Enqueuer или использовать TransactionalEnqueuer
* Реализовать интерфейс Queue
* Создать QueueRegistry и зарегистрировать инстансы Queue и Enqueuer
* Создать QueueExecutionPool и запустить его

### Spring конфигурация

Spring конфигурация избавляет от написания boilerplate кода путем конвенций.
Пример конфигурации - [example.SpringAutoConfiguration](https://github.com/yandex-money/db-queue/blob/master/src/test/java/example/SpringAutoConfiguration.java).

Spring конфигурацию логически можно разделить на две части:
* Базовая часть, которая отвечает за конфигурацию очередей - example.SpringAutoConfiguration.Base
* Бизнесовая часть, в которой задаётся логика работы очередей - example.SpringAutoConfiguration.Client

Для того, чтобы создать базовую часть, в контексте необходимо объявить несколько бинов:
* SpringQueueConfigContainer - настройки всех очередей, которые требуется запустить.
* SpringQueueCollector - Сборщик бинов, которые относятся к очередям.
* QueueExecutionPool - Управление запуском и остановом очередей.
* SpringQueueInitializer - Связывание бинов с друг другом и запуск по завершению создания контекста спринга.

Бизнесовая часть состоит в объявлении бинов Queue, Enqueuer и т.п.
Для объявления дааных бинов необходимо использовать классы с префиксом Spring.
Классы спринговой конфигурации реализуют SpringQueueIdentifiable,
за счет этого происходит связывание бинов по QueueLocation.

## Структура проекта

* _.internal.*

Внутренние классы реализации. 

*В классах данного пэкеджа, обратная совместимость 
между любыми релизами библиотеки не гарантируется*

* _.api

Клиенту библиотеки необходимо предоставить реализацию интерфейсов этого пакета. 
Содержит классы данных, участвующие в обработке очереди и постановке на выполнение.

* _.api.impl

Реализация по умолчанию для упрощения конфигурирования
и использования в наиболее частых вариантах использования.

* _.settings

Пакет с классами, отвечающими за настройку очередей.

* _.dao

Вспомогательные классы для управления и получения данных по очередям.
В случае стандартной реализации вам не потребуется доступ к методам данных классов.

* _.init

Классы данного пэкеджа необходимы для регистрации очередей и их запуска.

* _.spring

Классы отвечающие за spring конфигурацию очередей.
