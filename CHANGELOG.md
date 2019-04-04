## [6.1.1]() (04-04-2019)

В случае, если задача ставится в очередь до того, как полностью поднялся spring-контекст очередей,
внутри `SpringTransactionalProducer` лениво создавался `TransactionalProducer` со всеми полями `null`.
Таким образом очередь работать не будет.

Добавлен явный контроль полей `TransactionalProducer` на `requireNonNull`, чтобы вызывающий поток падал
с исключением, а сам `producer` лениво не созадавался, пока контекст очередей не поднят.
Тогда при следущем вызове `enqueue` очередь заработает.

## [6.1.0]() (19-03-2019)

* Переход на platformLibraryProjectVersion 3 версии

## [6.0.5]() (12-11-2018)

* Синхронизация тегов

## [6.0.4]() (12-11-2018)

* Проверка синхронизации

## [6.0.3]() (11-11-2018)

* Починил публикацию на bintray #2

## [6.0.2]() (11-11-2018)

* Починил публикацию на bintray

## [6.0.1]() (11-11-2018)

* Исправил сборку на github

## [6.0.0]() (11-11-2018)

* Переход на spring boot 2.1
* Публичный релиз библиотеки

## [5.0.0]() (28-08-2018)

* correlationId переименован в traceInfo

## [4.0.0]() (13-08-2018)

Изменён способ конфигурирования шардов БД. 

В предыдущей версии шарды были общими для всех очередей и идентичными между Producer и Consumer в пределах одной очереди.
Теперь используемые шарды задаются в каждом из QueueShardRouter и могут быть различными для Producer и Consumer.

Примеры новой конфигурации в тестах: example.ManualConfiguration и example.SpringAutoConfiguration

## [3.1.0]() (03-08-2018)

* Добавлена возможность будить поток разбора задача посредством QueueExecutionPool#wakeup

## [3.0.3]() (19-06-2018)

* Поддержка обновления контекста

## [3.0.2]() (21-05-2018)

* Добавил QueueStatisticsDao.getJdbcTemplate()

## [3.0.1]() (30-11-2017)

* Fix bug: if queue thread-count set greater than 1 than execution pool starts only one thread for queue processing

### [3.0.0]() (27-09-2017)

* QueueLocation replaced with QueueId wherever possible in order to identify particular queue.
* QueueDao instances are directly passed to SpringQueueInitializer instead of retrieving it from SpringQueueCollector.

### [2.0.0]() (04-09-2017)

* Removed TaskExecutionResult#fail(delay)
* Removed QueueSettings.AdditionalSetting
* Changed signature QueueDao#reenqueue
* Changed signature QueueActorDao#reenqueue
* Replaced AdditionalSetting#RETRY_FIXED_INTERVAL_DELAY with QueueSettings#getRetryInterval
* Added QueueSettings#getRetryInterval and QueueConfigsReader#SETTING_RETRY_INTERVAL
* Renamed TaskRetryType#FIXED_INTERVAL to TaskRetryType#LINEAR_BACKOFF
* Renamed QueueConfigsReader#VALUE_TASK_RETRY_TYPE_FIXED_INTERVAL to QueueConfigsReader#VALUE_TASK_RETRY_TYPE_LINEAR
* Changed value of QueueConfigsReader#VALUE_TASK_RETRY_TYPE_LINEAR to "linear"

### [1.0.0]() (01-09-2017)

* Stable version

### [0.0.11]() (29-08-2017)

* Add callback telling that SpringQueueConsumer and SpringQueueConsumer are initialized
* Fixed passing null property file to QueueConfigsReader

### [0.0.10]() (27-08-2017)

* Added ThreadLifecycleListener#executed to measure overall queue performance.
* Specifying ThreadLifecycleListener per queue

### [0.0.9]() (26-08-2017)

* Fixed ability to disable queue processing via thread count
* Renamed ThreadLifecycleListener#crashedOnPickedTask to ThreadLifecycleListener#crashed
* Constructors of builder classes are made private
* Return value of QueueProducer#enqueue changed to primitive
* Added reenqueue by actor via QueueActorDao#reenqueue

### [0.0.8]() (24-08-2017)

Renamed classes:
* Queue -> QueueConsumer
* Enqueuer -> QueueProducer
* ShardRouter -> QueueShardRouter
* QueueThreadLifecycleListener -> ThreadLifecycleListener
* QueueAction -> TaskExecutionResult
* PayloadTransformer -> TaskPayloadTransformer

### [0.0.7]() (24-08-2017)

* Upgrade Spring to 4.3.7

### [0.0.6]() (24-08-2017)

* Same queue names in different tables are forbidden
* Reading configuration from file (QueueConfigsReader)

### [0.0.5]() (20-08-2017)

* Translated README to English

### [0.0.1]() (08-08-2017)

* Initial version