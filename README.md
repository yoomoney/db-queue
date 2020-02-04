[![Build Status](https://travis-ci.org/yandex-money-tech/db-queue.svg?branch=master)](https://travis-ci.org/yandex-money-tech/db-queue)
[![Codecov](https://codecov.io/gh/yandex-money-tech/db-queue/branch/master/graph/badge.svg)](https://codecov.io/gh/yandex-money-tech/db-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Javadoc](https://img.shields.io/badge/javadoc-latest-blue.svg)](https://yandex-money-tech.github.io/db-queue/)
[![Download](https://api.bintray.com/packages/yandex-money-tech/maven/db-queue/images/download.svg)](https://bintray.com/yandex-money-tech/maven/db-queue/_latestVersion)
# Database Queue

Library provides worker-queue implementation on top of Java and database.

## Why?

There are several reasons:

* You need simple, efficient and flexible task processing tool which supports delayed job execution.
* You already have a database and don't want to introduce additional tools 
in your infrastructure (for example ActiveMq or RabbitMq) 
* You have somewhat small load. This worker queue mechanism can handle 
more than 1000 rps on single database and table. Moreover you can shard your database and get horizontal scalability. 
However we cannot guarantee that it would be easy to auto scale or handle more than 1000 rps.
* You require strong guaranties for task delivery or processing. Library offers at-least-once delivery semantic. 

## How it works?

1. You have a task that you want to process later. 
2. You tell [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) to schedule the task. 
3. [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) optionally chooses a database shard.
4. [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) converts the task payload to string representation through [TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html). 
5. [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) inserts the task in the database through [QueueDao](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/dao/QueueDao.html).
6. ... the task has been selected from database at specified time according to queue settings ... 
7. The task payload is converted to typed representation through [TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html).
8. The task is passed to the [QueueConsumer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueConsumer.html) instance in order to be processed. 
9. You process the task and return processing result. 
10. ... the task is updated according to processing result and queue settings ...

## Features

* Persistent working-queue
* Support for PostgreSQL, MSSQL.
* Storing queue tasks in a separate tables or in the same table ([QueueLocation](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueLocation.html)).
* Storing queue tasks in a separate databases for horizontal scaling ([QueueShard](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/config/QueueShard.html)).
* Delayed task execution.
* At-least-once task processing semantic.
* Several retry strategies in case of a task processing error ([TaskRetryType](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/TaskRetryType.html)).
* Task event listeners ([TaskLifecycleListener](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskLifecycleListener.html), [ThreadLifecycleListener](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/ThreadLifecycleListener.html)).
* Strong-typed api for task processing and enqueuing ([TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html)).
* Several task processing modes ([ProcessingMode](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/ProcessingMode.html)).
* And many other features, look at [Settings package](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/package-frame.html). 

## Database support

As of now the library supports PostgreSQL and MSSQL as backing database, however library architecture
makes it easy to add other relational databases which has support for transactions and "for update skip locked" feature,  
for example MySql, Oracle, H2.  
Feel free to add support for other databases via pull request.

## Dependencies

Library contains minimal set of dependencies.
It requires Spring Framework (spring-jdbc and spring-tx) for interacting with database. 
Other features of Spring ecosystem are not used. 

# Usage

## Versioning Rules

Project uses [Semantic Versioning](http://semver.org/).

## Dependency management

Library is available on [Bintray's JCenter repository](http://jcenter.bintray.com) 

```
<dependency>
  <groupId>com.yandex.money.tech</groupId>
  <artifactId>db-queue</artifactId>
  <version>8.3.0</version>
</dependency>
```

## Configuration

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
these recommendations. Settings heavily depends on a hardware 
and a load you have.

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

### Code

Example configuration is shown in [example.ExampleConfiguration](https://github.com/yandex-money-tech/db-queue/blob/master/src/test/java/example/ExampleConfiguration.java).

The main steps are:
* Specify a queue configuration through [QueueConfig](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueConfig.html) instance (or use [QueueConfigsReader](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueConfigsReader.html)).
* Implement [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) interface.
* Implement [QueueConsumer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueConsumer.html) interface.
* Create [QueueService](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/init/QueueService.html) and start it.
* Register `QueueConsumer` in `QueueService`
* Start queues through `QueueService`

## Project structure

* internal

Internal classes. **Not for public use**.

*Backward compatibility for classes in that package may be broken in any release*

* [api](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/package-frame.html)

You should provide implementation for interfaces in that package.
The package contains classes which are involved in processing or enqueueing tasks.

* [settings](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/package-frame.html)

Queue settings.

* [dao](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/dao/package-frame.html)

Additional classes for managing storage.

* [config](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/config/package-frame.html)

Registration and configuration.

# Known Issues

* Uneven load balancing

One of the hosts can consequently process several tasks very quickly while other hosts are sleeping.

* No support for Blue-green deployment

There is no support for blue-green deployment because a task is not bound to a host or to a group of hosts. 

* Hard to write tests.

Task processing is asynchronous. Therefore, it is hard to write tests because you always must think about that fact
and write code according to it. To ease development of tests you can use `wakeup` method of [QueueService](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/init/QueueService.html)

# How To Build

You can look at Travis (`.travis.yml`) or AppVeyor (`appveyor.yml`) configuration. 
We have two gradle build files. There are `build.gradle`, `gradlew`, `gradle/wrapper` for Yandex.Money infrastructure and
`build-public.gradle`, `gradlew-public`, `gradle-public/wrapper` for configuration outside of private network.

# How To Import Project in IDE

Unfortunately, there is a bug in IntelliJ IDEA (https://github.com/f0y/idea-two-gradle-builds) so you have to replace 
`gradle-public/wrapper/gradle-wrapper.properties` with `gradle/wrapper/gradle-wrapper.properties` and 
`build-public.gradle` with `build.gradle` before importing. 