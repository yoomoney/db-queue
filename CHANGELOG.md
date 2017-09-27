# Changelog

## Versioning Rules

Project uses [Semantic Versioning](http://semver.org/).

Given version x.y.z

* x - Major version. Backward compatibility is broken. Refactoring or big features.
* y - Minor version. Backward compatibile with previous version. New features.
* z - Patch version. Backward compatibile with previous version. Bug fix or small features. 

### [3.0.0]() (27-09-2017)

* QueueLocation replaced with QueueId wherever possible in order to identify particular queue.


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
