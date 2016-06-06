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

use Pheanstalk\Exception\ServerException;
use Pheanstalk\Pheanstalk;
use Pheanstalk\PheanstalkInterface;
use TYPO3\Flow\Annotations as Flow;
use Flowpack\JobQueue\Common\Exception as JobqueueException;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;

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
    protected $defaultTimeout = NULL;

    /**
     * @param string $name
     * @param array $options
     */
    public function __construct($name, array $options = array())
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        $clientOptions = isset($options['client']) ? $options['client'] : array();
        $host = isset($clientOptions['host']) ? $clientOptions['host'] : '127.0.0.1';
        $port = isset($clientOptions['port']) ? $clientOptions['port'] : PheanstalkInterface::DEFAULT_PORT;

        $this->client = new Pheanstalk($host, $port, $this->defaultTimeout);
    }

    /**
     * Publish a message to the queue
     *
     * @param Message $message
     * @return void
     */
    public function submit(Message $message)
    {
        $encodedMessage = $this->encodeMessage($message);
        $messageIdentifier = $this->client->putInTube($this->name, $encodedMessage);
        $message->setIdentifier($messageIdentifier);
        $message->setState(Message::STATE_SUBMITTED);
    }

    /**
     * Wait for a message in the queue and return the message for processing
     * (without safety queue)
     *
     * @param integer $timeout in seconds
     * @return Message The received message or NULL if a timeout occurred
     */
    public function waitAndTake($timeout = NULL)
    {
        if ($timeout === NULL) {
            $timeout = $this->defaultTimeout;
        }
        $pheanstalkJob = $this->client->reserveFromTube($this->name, $timeout);
        if ($pheanstalkJob === NULL || $pheanstalkJob === FALSE) {
            return NULL;
        }
        $message = $this->decodeMessage($pheanstalkJob->getData());
        $message->setIdentifier($pheanstalkJob->getId());
        $this->client->delete($pheanstalkJob);
        $message->setState(Message::STATE_DONE);
        return $message;

    }

    /**
     * Wait for a message in the queue and save the message to a safety queue
     *
     * TODO: Idea for implementing a TTR (time to run) with monitoring of safety queue. E.g.
     * use different queue names with encoded times?
     *
     * @param integer $timeout in seconds
     * @return Message
     */
    public function waitAndReserve($timeout = NULL)
    {
        if ($timeout === NULL) {
            $timeout = $this->defaultTimeout;
        }
        $pheanstalkJob = $this->client->reserveFromTube($this->name, $timeout);
        if ($pheanstalkJob === NULL || $pheanstalkJob === FALSE) {
            return NULL;
        }
        $message = $this->decodeMessage($pheanstalkJob->getData());
        $message->setIdentifier($pheanstalkJob->getId());
        return $message;
    }

    /**
     * Mark a message as finished
     *
     * @param Message $message
     * @return boolean TRUE if the message could be removed
     */
    public function finish(Message $message)
    {
        $messageIdentifier = $message->getIdentifier();
        $pheanstalkJob = $this->client->peek($messageIdentifier);
        $this->client->delete($pheanstalkJob);
        $message->setState(Message::STATE_DONE);
        return TRUE;
    }

    /**
     * Peek for messages
     * NOTE: The beanstalkd implementation only supports to peek the UPCOMING job, so this will throw an exception for
     * $limit != 1.
     *
     * @param integer $limit
     * @return array Messages or empty array if no messages were present
     * @throws JobqueueException
     */
    public function peek($limit = 1)
    {
        if ($limit !== 1) {
            throw new JobqueueException('The beanstalkd Jobqueue implementation currently only supports to peek one job at a time', 1352717703);
        }
        try {
            $pheanstalkJob = $this->client->peekReady($this->name);
        } catch (ServerException $exception) {
            return array();
        }
        if ($pheanstalkJob === NULL || $pheanstalkJob === FALSE) {
            return array();
        }

        $message = $this->decodeMessage($pheanstalkJob->getData());
        $message->setIdentifier($pheanstalkJob->getId());
        $message->setState(Message::STATE_SUBMITTED);
        return array($message);
    }

    /**
     * Count messages in the queue
     *
     * @return integer
     */
    public function count()
    {
        try {
            $clientStats = $this->client->statsTube($this->name);
            return (integer)$clientStats['current-jobs-ready'];
        } catch (ServerException $exception) {
            return 0;
        }
    }

    /**
     * Encode a message
     *
     * Updates the original value property of the message to resemble the
     * encoded representation.
     *
     * @param Message $message
     * @return string
     */
    protected function encodeMessage(Message $message)
    {
        $value = json_encode($message->toArray());
        $message->setOriginalValue($value);
        return $value;
    }

    /**
     * Decode a message from a string representation
     *
     * @param string $value
     * @return Message
     */
    protected function decodeMessage($value)
    {
        $decodedMessage = json_decode($value, TRUE);
        $message = new Message($decodedMessage['payload']);
        if (isset($decodedMessage['identifier'])) {
            $message->setIdentifier($decodedMessage['identifier']);
        }
        $message->setOriginalValue($value);
        return $message;
    }

    /**
     *
     * @param string $identifier
     * @return Message
     */
    public function getMessage($identifier)
    {
        $pheanstalkJob = $this->client->peek($identifier);
        return $this->decodeMessage($pheanstalkJob->getData());
    }


}