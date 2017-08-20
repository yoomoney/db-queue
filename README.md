[![Build Status](https://travis-ci.org/yandex-money/db-queue.svg?branch=master)](https://travis-ci.org/yandex-money/db-queue)
[![Build status](https://ci.appveyor.com/api/projects/status/8078bof07yp16112?svg=true)](https://ci.appveyor.com/project/f0y/db-queue)
[![Code Coverage](https://codecov.io/gh/yandex-money/db-queue/branch/feature/add_badges/graph/badge.svg)](https://codecov.io/gh/yandex-money/db-queue)
[![Codebeat](https://codebeat.co/badges/ff7a4c21-72fb-446c-b245-ba739240fe49)](https://codebeat.co/projects/github-com-yandex-money-db-queue-master)
[![Codacy](https://api.codacy.com/project/badge/Grade/3a0e23fae44843c284540929d750b65c)](https://www.codacy.com/app/f0y/db-queue?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=yandex-money/db-queue&amp;utm_campaign=Badge_Grade)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Download](https://api.bintray.com/packages/yandex-money/maven/db-queue/images/download.svg)](https://bintray.com/yandex-money/maven/db-queue/_latestVersion)

# Database Queue

Library provides worker-queue implementation on top of Java and database.

## Why?

There are several reasons:

* You need simple, efficient and flexible task processing tool which supports delayed job execution.
* You already have a database and don't want to introduce additional tools 
in your infrastructure (for example Kafka, RabbitMq, ...) 
* You have somewhat small load. This worker queue mechanism can handle 
more than 1000 rps on single database and table. Moreover you can shard your database and get horizontal scalability. 
However we cannot guarantee that it would be easy to auto scale or handle more than 1000 rps.
* You require strong guaranties for task delivery or processing. Library offers at-least-once delivery semantic. 

## How it works?

1. You have a task that you want to process later. 
2. You tell Enqueuer to schedule the task. 
3. Enqueuer chooses a database shard through ShardRouter.
4. Enqueuer converts the task payload to string representation through PayloadTransformer. 
5. Enqueuer inserts the task in the database through QueueDao.
6. ... the task has been selected from database in specified time ... 
7. The task payload is converted to typed representation through PayloadTransformer.
8. The task is passed to the Queue instance in order to be processed. 
9. You process the task and return processing result. 

## Features

* Support for PostgreSQL with version higher or equal to 9.5.
* Storing queue tasks in a separate tables or in the same table (QueueLocation).
* Storing queue tasks in a separate databases for horizontal scaling (ShardRouter).
* At-least-once task processing semantic (exactly-once in some cases).
* Delayed task execution.
* Several retry strategies in case of a task processing error (TaskRetryType).
* Task event listeners (TaskLifecycleListener, QueueThreadLifecycleListener).
* Strong-typed api for task processing and enqueuing (PayloadTransformer).
* Several task processing modes (ProcessingMode).

## Dependencies

Library supports only PostgreSQL as backing database, however library architecture
makes it easy to add other databases which has "skip locked" feature.

Furthermore, library requires Spring Framework (spring-jdbc and spring-tx) for interacting with database. 
Spring-context is used only for alternative configuration and can be safely excluded from runtime dependencies.

# Usage

## Versioning Rules

Project uses [Semantic Versioning](http://semver.org/).

## Dependency management

Library is available on [Bintray's JCenter repository](http://jcenter.bintray.com) 

```
<dependency>
  <groupId>ru.yandex.money.common</groupId>
  <artifactId>db-queue</artifactId>
  <version>0.0.5</version>
</dependency>
```

## Configuration

* Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id            BIGSERIAL PRIMARY KEY,
  queue_name    VARCHAR(128) NOT NULL,
  task          TEXT,
  create_time   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  process_time  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt       INTEGER                  DEFAULT 0,
  actor         VARCHAR(128),
  log_timestamp VARCHAR(128)
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON tasks (queue_name, process_time, id DESC);
```
* Specify a queue configuration through QueueConfig instance.
  * Choose name for the queue.
  * Specify betweenTaskTimeout and noTaskTimeout settings in QueueSettings instance.
* Use manual or spring-auto configuration.

### Manual configuration

Manual configuration may be used in cases where you don't have spring context.
It offers more flexibility than spring configuration.
Example - [example.ManualConfiguration](https://github.com/yandex-money/db-queue/blob/master/src/test/java/example/ManualConfiguration.java).

Main steps to create manual configuration:

* Create QueueDao instance for each shard.
* Implement ShardRouter interface or use SingleShardRouter.
* Implement PayloadTransformer interface or use NoopPayloadTransformer.
* Implement Enqueuer interface or use TransactionalEnqueuer.
* Implement Queue interface.
* Create QueueRegistry and register Queue and Enqueuer instances.
* Create QueueExecutionPool and start it.

### Spring-Auto Configuration

Spring configuration is more lightweight than manual configuration.
Example - [example.SpringAutoConfiguration](https://github.com/yandex-money/db-queue/blob/master/src/test/java/example/SpringAutoConfiguration.java).

Spring configuration can be divided in two parts:

* Base configuration. You may put it in your common code - example.SpringAutoConfiguration.Base
* Client configuration specifies how your queues will work - example.SpringAutoConfiguration.Client

Base configuration includes several beans:

* SpringQueueConfigContainer - Provides settings for all queues in your spring context.
* SpringQueueCollector - Collects beans related to spring configuration.
* SpringQueueInitializer - Wires queue beans to each other and starts queues.

In client configuration you must use classes with prefix Spring.

## Project structure

* _.internal.*

Internal classes. **Not for public use**.

*Backward compatibility for classes in that package may be broken in any release*

* _.api

You should provide implementation for interfaces in that package.
The package contains classes which are involved in processing or enqueueing tasks.

* _.api.impl

Default implementation for api interfaces. 
Allows easier configuration in common use cases.

* _.settings

Queue settings.

* _.dao

Additional classes for queue managing and statistics retrieval.
In common use cases you don't need to use that classes.

* _.init

Registration and running queues.

* _.spring

Classes related to Spring configuration.
