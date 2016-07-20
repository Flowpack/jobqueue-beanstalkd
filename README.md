# Flowpack.JobQueue.Beanstalkd

A job queue backend for the [Flowpack.JobQueue.Common](https://github.com/Flowpack/jobqueue-common) package based on [beanstalkd](http://kr.github.io/beanstalkd/).

## Usage

Install the package using composer:

```
composer require flowpack/jobqueue-beanstalkd
```

If not already installed, that will fetch its requirements, namely `jobqueue-common` and `pda/pheanstalk`.
*NOTE:* This package needs a running [beanstalkd](http://kr.github.io/beanstalkd/) server

Now the queue can be configured like this:

```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'some-queue':
          className: 'Flowpack\JobQueue\Beanstalkd\Queue\BeanstalkdQueue'
          executeIsolated: true
          options:
            client:
              host: 127.0.0.11
              port: 11301
            defaultTimeout: 50
          releaseOptions:
            priority: 512
            delay: 120
```

## Specific options

The `BeanstalkdQueue` supports following options:

| Option                  | Type    | Default                                  | Description                              |
| ----------------------- |---------| ----------------------------------------:| ---------------------------------------- |
| defaultTimeout          | integer | 60                                       | Number of seconds new messages are waited for before a timeout occurs (This is overridden by a "timeout" argument in the `waitAndTake()` and `waitAndReserve()` methods |
| client                  | array   | ['host' => '127.0.0.1', 'port' => 11300] | Beanstalkd connection settings |

### Submit options

Additional options supported by `JobManager::queue()`, `BeanstalkdQueue::submit()` and the `Job\Defer` annotation:

| Option                  | Type    | Default          | Description                              |
| ----------------------- |---------| ----------------:| ---------------------------------------- |
| delay                   | integer | 0                | Number of seconds before a message is marked "ready" after submission. This can be useful to prevent premature execution of jobs (i.e. before entites are persisted) |
| priority                | integer | 1024             | Priority of the message. most urgent: 0, least urgent: 4294967295 |
| ttr                     | integer | 60               | Number of seconds a message is allowed to be reserved before it is released, aborted or finished. NOTE: This option is not available in the *releaseOptions*! |

### Release options

Additional options to be specified via `releaseOptions`: 

| Option                  | Type    | Default          | Description                              |
| ----------------------- |---------| ----------------:| ---------------------------------------- |
| delay                   | integer | 0                | Number of seconds before a message is marked "ready" after it has been released. |
| priority                | integer | 1024             | Priority of the message. most urgent: 0, least urgent: 4294967295 |

## License

This package is licensed under the MIT license

## Contributions

Pull-Requests are more than welcome. Make sure to read the [Code Of Conduct](CodeOfConduct.rst).