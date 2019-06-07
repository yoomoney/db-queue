[![Build Status](https://travis-ci.org/yandex-money-tech/db-queue.svg?branch=master)](https://travis-ci.org/yandex-money-tech/db-queue)
[![Build status](https://ci.appveyor.com/api/projects/status/2ee4wumomugjnnl7?svg=true)](https://ci.appveyor.com/project/f0y/db-queue)
[![codecov](https://codecov.io/gh/yandex-money-tech/db-queue/branch/master/graph/badge.svg)](https://codecov.io/gh/yandex-money-tech/db-queue)
[![Codebeat](https://codebeat.co/badges/ff7a4c21-72fb-446c-b245-ba739240fe49)](https://codebeat.co/projects/github-com-yandex-money-db-queue-master)
[![Codacy](https://api.codacy.com/project/badge/Grade/78c05240110c427f873895a5a3ee1eae)](https://www.codacy.com/project/f0y/db-queue/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=yandex-money-tech/db-queue&amp;utm_campaign=Badge_Grade_Dashboard)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Javadoc](https://img.shields.io/badge/javadoc-latest-blue.svg)](https://yandex-money-tech.github.io/db-queue/)
[![Download](https://api.bintray.com/packages/yandex-money-tech/maven/db-queue/images/download.svg)](https://bintray.com/yandex-money-tech/maven/db-queue/_latestVersion)
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
2. You tell [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) to schedule the task. 
3. [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) chooses a database shard through [QueueShardRouter](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueShardRouter.html).
4. [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) converts the task payload to string representation through [TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html). 
5. [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) inserts the task in the database through [QueueDao](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/dao/QueueDao.html).
6. ... the task has been selected from database in specified time ... 
7. The task payload is converted to typed representation through [TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html).
8. The task is passed to the [QueueConsumer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueConsumer.html) instance in order to be processed. 
9. You process the task and return processing result. 

## Features

* Persistence working-queue
* Support for PostgreSQL with version higher or equal to 9.5.
* Storing queue tasks in a separate tables or in the same table ([QueueLocation](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueLocation.html)).
* Storing queue tasks in a separate databases for horizontal scaling ([QueueShardRouter](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueShardRouter.html)).
* Delayed task execution.
* At-least-once task processing semantic.
* Several retry strategies in case of a task processing error ([TaskRetryType](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/TaskRetryType.html)).
* Task event listeners ([TaskLifecycleListener](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskLifecycleListener.html), [ThreadLifecycleListener](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/ThreadLifecycleListener.html)).
* Strong-typed api for task processing and enqueuing ([TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html)).
* Several task processing modes ([ProcessingMode](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/ProcessingMode.html)).

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
  <groupId>com.yandex.money.tech</groupId>
  <artifactId>db-queue</artifactId>
  <version>7.0.0</version>
</dependency>
```

## Configuration

* Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id                BIGSERIAL PRIMARY KEY,
  queue_name        VARCHAR(128) NOT NULL,
  task              TEXT,
  create_time       TIMESTAMP WITH TIME ZONE DEFAULT now(),
  process_time      TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt           INTEGER                  DEFAULT 0,
  reenqueue_attempt INTEGER                  DEFAULT 0,
  total_attempt     INTEGER                  DEFAULT 0,
  actor             VARCHAR(128),
  log_timestamp     TEXT
) WITH (fillfactor = 80);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, process_time, id DESC);
```
* Specify a queue configuration through [QueueConfig](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueConfig.html) instance (or use [QueueConfigsReader](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueConfigsReader.html)).
  * Choose name for the queue.
  * Specify [betweenTaskTimeout](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueSettings.html#getBetweenTaskTimeout) and [noTaskTimeout](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/QueueSettings.html#getNoTaskTimeout) settings.
* Use manual or spring-auto configuration.

### Manual configuration

Manual configuration may be used in cases where you don't have spring context.
It offers more flexibility than spring configuration.
Example - [example.ManualConfiguration](https://github.com/yandex-money/db-queue/blob/master/src/test/java/example/ManualConfiguration.java).

Main steps to create manual configuration:

* Implement [QueueShardRouter](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueShardRouter.html) interface
* Implement [TaskPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/TaskPayloadTransformer.html) interface or use [NoopPayloadTransformer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/impl/NoopPayloadTransformer.html).
* Implement [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) interface or use [TransactionalProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/impl/TransactionalProducer.html).
* Implement [QueueConsumer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueConsumer.html) interface.
* Create [QueueRegistry](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/init/QueueRegistry.html) and register [QueueConsumer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueConsumer.html) and [QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) instances.
* Create [QueueExecutionPool](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/init/QueueExecutionPool.html) and start it.

### Spring-Auto Configuration

Spring configuration is more lightweight than manual configuration.
Example - [example.SpringAutoConfiguration](https://github.com/yandex-money-tech/db-queue/blob/master/src/test/java/example/SpringAutoConfiguration.java).

Spring configuration can be divided in two parts:

* Base configuration. You may put it in your common code - _example.SpringAutoConfiguration.Base_
* Client configuration specifies how your queues will work - _example.SpringAutoConfiguration.Client_

Base configuration includes several beans:

* [SpringQueueConfigContainer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/spring/SpringQueueConfigContainer.html) - Provides settings for all queues in your spring context.
* [SpringQueueCollector](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/spring/SpringQueueCollector.html) - Collects beans related to spring configuration.
* [SpringQueueInitializer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/spring/SpringQueueInitializer.html) - Wires queue beans to each other and starts queues.

In client configuration you must use classes with prefix Spring.

## Project structure

* internal

Internal classes. **Not for public use**.

*Backward compatibility for classes in that package may be broken in any release*

* [api](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/package-frame.html)

You should provide implementation for interfaces in that package.
The package contains classes which are involved in processing or enqueueing tasks.

* [api.impl](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/impl/package-frame.html)

Default implementation for api interfaces. Allows easy configuration in common use cases.

* [settings](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/settings/package-frame.html)

Queue settings.

* [dao](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/dao/package-frame.html)

Additional classes for queue managing and statistics retrieval.
In common use cases you don't need to use that classes.

* [init](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/init/package-frame.html)

Registration and running queues.

* [spring](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/spring/package-frame.html)

Classes related to Spring configuration.

* [spring.impl](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/spring/impl/package-frame.html)

Default implementation for Spring configuration. Allows easy configuration in common use cases.

## Database Tuning

### PostgreSQL

You should always analyze your database workload before applying 
this recommendations. These settings heavily depends on a hardware 
and a load you have.

#### Fill Factor 

You need to set a low fill-factor for table in order to 
let database put row updates to the same page.
In that case database will need less amount of random page writes. 
This technique also prevents fragmentation so we get more robust selects. 
Same rules are applied to an indexes. You can safely set fill-factor 
for tables and indexes to 70%. 

#### Autovacuum

You need to make autovacuum more aggressive in order to eliminate dead tuples. 
Dead tuples leads to excessive page reads because they occupy space 
that can be reused by active tuples. Autovacuum can be configured in many ways, 
for example, you can set 
[scale-factor](https://www.postgresql.org/docs/current/static/runtime-config-autovacuum.html#GUC-AUTOVACUUM-VACUUM-SCALE-FACTOR) to 1%.

# Known Issues

* Retry strategies cannot be defined by a user

In some cases a you may want to use different retry strategies. 
For example, do first retry almost immediately and then use standard behaviour.
Library does not support this type of customization.

* Uneven load balancing

One of the hosts can consequently process several tasks very quickly while other hosts are sleeping.

* Max throughput is limited by "between task timeout"

Thread falls asleep for "between task timeout" regardless of task processing result. 
Although, it can pick next task after successful result and do processing.

* No support for Blue-green deployment

There is no support for blue-green deployment because a task is not bound to a host or to a group of hosts. 

* No support for failover.

[QueueProducer](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/api/QueueProducer.html) can fail on task scheduling. You can manually detect that fail is caused by database 
and try insert task on next shard.

* Hard to write tests.

Task processing is asynchronous. Therefore, it is hard to write tests because you always must think about that fact
and write code according to it. To ease development of tests you can use `wakeup` method of [QueueExecutionPool](https://yandex-money-tech.github.io/db-queue/ru/yandex/money/common/dbqueue/init/QueueExecutionPool.html)

# How To Build

You can look at Travis (`.travis.yml`) or AppVeyor (`appveyor.yml`) configuration. 
We have two gradle build files. There are `build.gradle`, `gradlew`, `gradle/wrapper` for Yandex.Money infrastructure and
`build-public.gradle`, `gradlew-public`, `gradle-public/wrapper` for configuration outside of private network.

# How To Import Project in IDE

Unfortunately, there is a bug in IntelliJ IDEA (https://github.com/f0y/idea-two-gradle-builds) so you have to replace 
`gradle-public/wrapper/gradle-wrapper.properties` with `gradle/wrapper/gradle-wrapper.properties` and 
`build-public.gradle` with `build.gradle` before importing. 