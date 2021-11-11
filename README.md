[![Build Status](https://travis-ci.com/yoomoney/db-queue.svg?branch=master)](https://travis-ci.com/github/yoomoney/db-queue/branches)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Javadoc](https://img.shields.io/badge/javadoc-latest-blue.svg)](https://yoomoney.github.io/db-queue/)
[![Download](https://img.shields.io/badge/Download-latest)](https://search.maven.org/artifact/ru.yoomoney.tech/db-queue)

# Database Queue

Library provides worker-queue implementation on top of Java and database.

Fintech company [YooMoney](https://yoomoney.ru/) uses db-queue in cases where reliability is a must-have requirement.

Project uses [Semantic Versioning](http://semver.org/).  
Library is available on [Maven Central](https://search.maven.org/)

```
implementation 'ru.yoomoney.tech:db-queue:15.0.0'
```

## Why?

There are several reasons:

* You need simple, efficient and flexible task processing tool which supports delayed job execution.
* You already have a database and don't want to introduce additional tools in your infrastructure.
* You have somewhat small load. Db-queue can handle more than 1000 rps on a single table. Moreover, you can shard your
  database and get horizontal scalability.
* You require strong guaranties for task delivery, processing and consistency.

## Features

* Persistent working-queue
* Support for PostgreSQL, Oracle, MSSQL, H2.
* At-least-once task processing semantic.
* Delayed task execution.
* Strong-typed
  api ([TaskPayloadTransformer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/TaskPayloadTransformer.java))
* Task processing
  modes ([ProcessingSettings](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/ProcessingSettings.java))
* Task polling
  settings ([PollSettings](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/PollSettings.java))
* Retry strategies in case of
  errors ([FailureSettings](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/FailureSettings.java))
* Retry strategies when you need postpone
  tasks ([ReenqueueSettings](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/ReenqueueSettings.java))
* Tracing support
  via [Brave](https://github.com/openzipkin/brave) ([ExampleTracingConfiguration](db-queue-test/src/test/java/ru/yoomoney/tech/dbqueue/test/ExampleTracingConfiguration.java))
* Task event listeners to build up
  monitoring ([TaskLifecycleListener](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/TaskLifecycleListener.java)
  ,
  [ThreadLifecycleListener](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/ThreadLifecycleListener.java))
* Configuration reload in
  runtime ([QueueService#updateQueueConfigs](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/QueueService.java))
* Reading queue configuration from file and dynamic reloading when file changed
  ([QueueConfigsReader](src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueConfigsReader.java),
  [QueueConfigsReloader](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueConfigsReloader.java)).
* Storing queue tasks in a separate
  tables ([QueueLocation](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueLocation.java)).
* Storing queue tasks in a separate
  databases ([QueueShard](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/QueueShard.java)).
* And many other features

The library provides one-time tasks - tasks that are executed once.
If you need (recurring tasks)/(periodic tasks) - tasks that are executed periodically, look at 
[db-queue-scheduler](https://github.com/yoomoney/db-queue-scheduler) library, please.

# Usage

## How it works?

1. You have a task that you want to process later.
2. You tell [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) to schedule the
   task.
3. QueueProducer chooses a database shard.
4. QueueProducer converts the task payload to string representation
   through [TaskPayloadTransformer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/TaskPayloadTransformer.java)
   .
5. QueueProducer inserts the task in the database
   through [QueueDao](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/dao/QueueDao.java).
6. ... the task has been selected from database at specified time according to queue settings ...
7. The task payload is converted to typed representation
   through [TaskPayloadTransformer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/TaskPayloadTransformer.java)
   .
8. The task is passed to
   the [QueueConsumer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueConsumer.java) instance in order to
   be processed.
9. You process the task and return processing result.
10. ... the task is updated according to processing result and queue settings ...

## Code configuration

* Simple
  configuration: [ExampleBasicConfiguration](db-queue-test/src/test/java/ru/yoomoney/tech/dbqueue/test/ExampleBasicConfiguration.java)
  .
* Tracing
  configuration: [ExampleTracingConfiguration](db-queue-test/src/test/java/ru/yoomoney/tech/dbqueue/test/ExampleTracingConfiguration.java)

The main steps to configure the library:

* Specify a queue configuration
  through [QueueConfig](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueConfig.java).
* Implement [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) interface.
* Implement [QueueConsumer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueConsumer.java) interface.
* Create [QueueService](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/QueueService.java).
* Register `QueueConsumer` in `QueueService`
* Start queues through `QueueService`

## Database configuration

As of now the library supports PostgreSQL, MSSQL, Oracle and H2 as backing database, however library architecture makes
it easy to add other relational databases which has support for transactions and "for update skip locked" feature,  
for example MySql.  
Feel free to add support for other databases via pull request.

### PostgreSQL

Create table (with index) where tasks will be stored.

```sql
CREATE TABLE queue_tasks (
  id                BIGSERIAL PRIMARY KEY,
  queue_name        TEXT NOT NULL,
  payload           TEXT,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt           INTEGER                  DEFAULT 0,
  reenqueue_attempt INTEGER                  DEFAULT 0,
  total_attempt     INTEGER                  DEFAULT 0
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks USING btree (queue_name, next_process_at, id DESC);
```

You should always analyze your database workload before applying 
these recommendations. Settings heavily depends on a hardware, and a load you have.

* Fill Factor 

You need to set a low fill-factor for table in order to 
let database put row updates to the same page.
In that case database will need less amount of random page writes. 
This technique also prevents fragmentation so we get more robust selects. 
Same rules are applied to an indexes. You can safely set fill-factor 
for tables and indexes to 30%.

Our production settings for frequently updated tasks table are:
```sql
CREATE TABLE queue_tasks (...) WITH (fillfactor=30)
CREATE INDEX ... ON queue_tasks USING btree (...) WITH (fillfactor=30)
``` 

* Autovacuum

You need to make autovacuum more aggressive in order to eliminate dead tuples. 
Dead tuples leads to excessive page reads because they occupy space 
that can be reused by active tuples. Autovacuum can be configured in many ways, 
for example, you can set 
[scale-factor](https://www.postgresql.org/docs/current/static/runtime-config-autovacuum.html#GUC-AUTOVACUUM-VACUUM-SCALE-FACTOR) to 1% or even lower.

Our production settings for frequently updated tasks tables are:
```sql
CREATE TABLE queue_tasks (...) WITH (
autovacuum_vacuum_cost_delay=5, 
autovacuum_vacuum_cost_limit=500,
autovacuum_vacuum_scale_factor=0.0001)
```

### MSSQL

Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id                INT IDENTITY(1,1) NOT NULL,
  queue_name        TEXT NOT NULL,
  payload           TEXT,
  created_at        DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET(),
  next_process_at   DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET(),
  attempt           INTEGER NOT NULL         DEFAULT 0,
  reenqueue_attempt INTEGER NOT NULL         DEFAULT 0,
  total_attempt     INTEGER NOT NULL         DEFAULT 0,
  PRIMARY KEY (id)
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, next_process_at, id DESC);
```

### Oracle

Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id                NUMBER(38) NOT NULL PRIMARY KEY,
  queue_name        VARCHAR2(128) NOT NULL,
  payload           CLOB,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  attempt           NUMBER(38)                  DEFAULT 0,
  reenqueue_attempt NUMBER(38)                  DEFAULT 0,
  total_attempt     NUMBER(38)                  DEFAULT 0
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, next_process_at, id DESC);
```
Create sequence and specify its name through `QueueLocation.Builder.withIdSequence(String)` 
or `id-sequence` in file config.
```sql
CREATE SEQUENCE tasks_seq;
```

### H2 database

A table that is needed for a work 
```sql
CREATE TABLE queue_tasks (
  id                BIGSERIAL PRIMARY KEY,
  queue_name        VARCHAR(100) NOT NULL,
  payload           VARCHAR(100),
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt           INTEGER                  DEFAULT 0,
  reenqueue_attempt INTEGER                  DEFAULT 0,
  total_attempt     INTEGER                  DEFAULT 0
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, next_process_at, id DESC);
```

## Modularity

The library is divided into several modules. Each module contains minimal set of dependencies to easily integrate in any
project.

* `db-queue-core` module provides base logic and requires `org.slf4j:slf4j-api` library
* `db-queue-spring` module provides access to database and requires Spring Framework: spring-jdbc and spring-tx. Other
  features of Spring ecosystem are not in use.
* `db-queue-brave` module provides tracing support with help of [Brave](https://github.com/openzipkin/brave)
* `db-queue-test` module provides integration testing across all modules. It might help to figure out how to use the
  library in your code.

## Project structure

* [api](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api)

You should provide implementation for interfaces in that package.
The package contains classes which are involved in processing or enqueueing tasks.

* [settings](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings)

Queue settings.

* [dao](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/dao)

Additional classes for managing storage.

* [config](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config)

Registration and configuration.

* internal

Internal classes. **Not for public use**.

*Backward compatibility for classes in that package maybe broken in any release*