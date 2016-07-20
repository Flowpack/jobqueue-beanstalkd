<?php
namespace Flowpack\JobQueue\Beanstalkd\Queue;

/*
 * This file is part of the Flowpack.JobQueue.Beanstalkd package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Pheanstalk\Exception as PheanstalkException;
use Pheanstalk\Exception\ServerException;
use Pheanstalk\Pheanstalk;
use Pheanstalk\PheanstalkInterface;
use TYPO3\Flow\Annotations as Flow;

/**
 * A queue implementation using beanstalkd as the queue backend
 *
 * Depends on Pheanstalk as the PHP beanstalkd client.
 */
class BeanstalkdQueue implements QueueInterface
{

    /**
     * @var string
     */
    protected $name;

    /**
     * @var Pheanstalk
     */
    protected $client;

    /**
     * Default connect timeout in seconds
     *
     * @var integer
     */
    protected $defaultTimeout = null;

    /**
     * @param string $name
     * @param array $options
     */
    public function __construct($name, array $options = [])
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        $clientOptions = isset($options['client']) ? $options['client'] : [];
        $host = isset($clientOptions['host']) ? $clientOptions['host'] : '127.0.0.1';
        $port = isset($clientOptions['port']) ? $clientOptions['port'] : PheanstalkInterface::DEFAULT_PORT;

        $this->client = new Pheanstalk($host, $port, $this->defaultTimeout);
    }

    /**
     * @inheritdoc
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @inheritdoc
     */
    public function submit($payload, array $options = [])
    {
        $priority = isset($options['priority']) ? (integer)$options['priority'] : PheanstalkInterface::DEFAULT_PRIORITY;
        $delay = isset($options['delay']) ? (integer)$options['delay'] : PheanstalkInterface::DEFAULT_DELAY;
        $ttr = isset($options['ttr']) ? (integer)$options['ttr'] : PheanstalkInterface::DEFAULT_TTR;
        return (string)$this->client->putInTube($this->name, json_encode($payload), $priority, $delay, $ttr);
    }

    /**
     * @inheritdoc
     */
    public function waitAndTake($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $pheanstalkJob = $this->client->reserveFromTube($this->name, $timeout);
        if ($pheanstalkJob === null || $pheanstalkJob === false) {
            return null;
        }
        $this->client->delete($pheanstalkJob);
        return new Message((string)$pheanstalkJob->getId(), json_decode($pheanstalkJob->getData(), true));
    }

    /**
     * @inheritdoc
     */
    public function waitAndReserve($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $pheanstalkJob = $this->client->reserveFromTube($this->name, $timeout);
        if ($pheanstalkJob === null || $pheanstalkJob === false) {
            return null;
        }
        $pheanstalkJobStats = $this->client->statsJob($pheanstalkJob);
        $numberOfReleases = isset($pheanstalkJobStats['reserves']) && $pheanstalkJobStats['reserves'] > 0 ? (integer)$pheanstalkJobStats['reserves'] - 1 : 0;
        return new Message((string)$pheanstalkJob->getId(), json_decode($pheanstalkJob->getData(), true), $numberOfReleases);
    }

    /**
     * @inheritdoc
     */
    public function release($messageId, array $options = [])
    {
        $pheanstalkJob = $this->client->peek((integer)$messageId);
        $priority = isset($options['priority']) ? $options['priority'] : PheanstalkInterface::DEFAULT_PRIORITY;
        $delay = isset($options['delay']) ? $options['delay'] : PheanstalkInterface::DEFAULT_DELAY;
        $this->client->release($pheanstalkJob, $priority, $delay);
    }

    /**
     * @inheritdoc
     */
    public function abort($messageId)
    {
        $pheanstalkJob = $this->client->peek((integer)$messageId);
        $this->client->bury($pheanstalkJob);
    }

    /**
     * @inheritdoc
     */
    public function finish($messageId)
    {
        $pheanstalkJob = $this->client->peek((integer)$messageId);
        $this->client->delete($pheanstalkJob);
        return true;
    }

    /**
     * @inheritdoc
     * NOTE: The beanstalkd implementation only supports to peek the UPCOMING job, so this will throw an exception for $limit != 1.
     *
     * @throws JobQueueException
     */
    public function peek($limit = 1)
    {
        if ($limit !== 1) {
            throw new JobQueueException('The beanstalkd Jobqueue implementation currently only supports to peek one job at a time', 1352717703);
        }
        try {
            $pheanstalkJob = $this->client->peekReady($this->name);
        } catch (ServerException $exception) {
            return [];
        }
        if ($pheanstalkJob === null || $pheanstalkJob === false) {
            return [];
        }

        return [new Message((string)$pheanstalkJob->getId(), json_decode($pheanstalkJob->getData(), true))];
    }

    /**
     * @inheritdoc
     */
    public function count()
    {
        try {
            $clientStats = $this->client->statsTube($this->name);
            return (integer)$clientStats['current-jobs-ready'];
        } catch (PheanstalkException $exception) {
            return 0;
        }
    }

    /**
     * @return void
     */
    public function setUp()
    {
        $this->client->useTube($this->name);
    }

    /**
     * @inheritdoc
     */
    public function flush()
    {
        try {
            while (true) {
                $job = $this->client->peekDelayed($this->name);
                $this->client->delete($job);
            }
        } catch (\Exception $e) {
        }
        try {
            while (true) {
                $job = $this->client->peekBuried($this->name);
                $this->client->delete($job);
            }
        } catch (\Exception $e) {
        }
        try {
            while (true) {
                $job = $this->client->peekReady($this->name);
                $this->client->delete($job);
            }
        } catch (\Exception $e) {
        }
    }
}