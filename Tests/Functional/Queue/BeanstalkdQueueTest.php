<?php
namespace Flowpack\JobQueue\Beanstalkd\Tests\Functional\Queue;

/*
 * This file is part of the Flowpack.JobQueue.Beanstalkd package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Pheanstalk\Pheanstalk;
use TYPO3\Flow\Configuration\ConfigurationManager;
use TYPO3\Flow\Tests\FunctionalTestCase;
use Flowpack\JobQueue\Beanstalkd\Queue\BeanstalkdQueue;
use Flowpack\JobQueue\Common\Queue\Message;

/**
 * Functional test for BeanstalkdQueue
 */
class BeanstalkdQueueTest extends FunctionalTestCase
{

    /**
     * @var BeanstalkdQueue
     */
    protected $queue;

    /**
     * @var Pheanstalk
     */
    protected $client;

    /**
     * Set up dependencies
     */
    public function setUp()
    {
        parent::setUp();
        $configurationManager = $this->objectManager->get('TYPO3\Flow\Configuration\ConfigurationManager');
        $settings = $configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'Flowpack.JobQueue.Beanstalkd');
        if (!isset($settings['testing']['enabled']) || $settings['testing']['enabled'] !== TRUE) {
            $this->markTestSkipped('beanstalkd is not configured (FlowPack.Jobqueue.Beanstalkd.testing.enabled != TRUE)');
        }

        $queueName = 'Test-queue';
        $this->queue = new BeanstalkdQueue($queueName, $settings['testing']);

        $clientOptions = $settings['testing']['client'];
        $host = isset($clientOptions['host']) ? $clientOptions['host'] : '127.0.0.1';
        $port = isset($clientOptions['port']) ? $clientOptions['port'] : '11300';
        $this->client = new Pheanstalk($host, $port);

        // flush queue:
        try {
            while (true) {
                $job = $this->client->peekDelayed($queueName);
                $this->client->delete($job);
            }
        } catch (\Exception $e) {
        }
        try {
            while (true) {
                $job = $this->client->peekBuried($queueName);
                $this->client->delete($job);
            }
        } catch (\Exception $e) {
        }
        try {
            while (true) {
                $job = $this->client->peekReady($queueName);
                $this->client->delete($job);
            }
        } catch (\Exception $e) {
        }
    }

    /**
     * @test
     */
    public function publishAndWaitWithMessageWorks()
    {
        $message = new Message('Yeah, tell someone it works!');
        $this->queue->submit($message);

        $result = $this->queue->waitAndTake(1);
        $this->assertNotNull($result, 'wait should receive message');
        $this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');
    }

    /**
     * @test
     */
    public function waitForMessageTimesOut()
    {
        $result = $this->queue->waitAndTake(1);
        $this->assertNull($result, 'wait should return NULL after timeout');
    }

    /**
     * @test
     */
    public function peekReturnsNextMessagesIfQueueHasMessages()
    {
        $message = new Message('First message');
        $this->queue->submit($message);
        $message = new Message('Another message');
        $this->queue->submit($message);

        $results = $this->queue->peek(1);
        $this->assertEquals(1, count($results), 'peek should return a message');
        /** @var Message $result */
        $result = $results[0];
        $this->assertEquals('First message', $result->getPayload());
        $this->assertEquals(Message::STATE_SUBMITTED, $result->getState(), 'Message state should be submitted');

        $results = $this->queue->peek(1);
        $this->assertEquals(1, count($results), 'peek should return a message again');
        $result = $results[0];
        $this->assertEquals('First message', $result->getPayload(), 'second peek should return the same message again');
    }

    /**
     * @test
     */
    public function peekReturnsNullIfQueueHasNoMessage()
    {
        $result = $this->queue->peek();
        $this->assertEquals(array(), $result, 'peek should not return a message');
    }

    /**
     * @test
     */
    public function waitAndReserveWithFinishRemovesMessage()
    {
        $message = new Message('First message');
        $this->queue->submit($message);


        $result = $this->queue->waitAndReserve(1);
        $this->assertNotNull($result, 'waitAndReserve should receive message');
        $this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');

        $result = $this->queue->peek();
        $this->assertEquals(array(), $result, 'no message should be present in queue');

        $finishResult = $this->queue->finish($message);
        $this->assertTrue($finishResult, 'message should be finished');
    }

    /**
     * @test
     */
    public function countReturnsZeroByDefault()
    {
        $this->assertSame(0, $this->queue->count());
    }

    /**
     * @test
     */
    public function countReturnsNumberOfReadyJobs()
    {
        $message1 = new Message('First message');
        $this->queue->submit($message1);

        $message2 = new Message('Second message');
        $this->queue->submit($message2);

        $this->assertSame(2, $this->queue->count());
    }

}