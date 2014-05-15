<?php
namespace TYPO3\Jobqueue\Beanstalkd\Tests\Functional\Queue;

/*                                                                            *
 * This script belongs to the TYPO3 Flow package "TYPO3.Jobqueue.Beanstalkd". *
 *                                                                            *
 * It is free software; you can redistribute it and/or modify it under        *
 * the terms of the GNU General Public License, either version 3 of the       *
 * License, or (at your option) any later version.                            *
 *                                                                            *
 * The TYPO3 project - inspiring people to share!                             *
 *                                                                            */

use TYPO3\Flow\Configuration\ConfigurationManager;
use TYPO3\Flow\Tests\FunctionalTestCase;
use TYPO3\Jobqueue\Beanstalkd\Queue\BeanstalkdQueue;
use TYPO3\Jobqueue\Common\Queue\Message;

/**
 * Functional test for BeanstalkdQueue
 */
class BeanstalkdQueueTest extends FunctionalTestCase {

	/**
	 * @var BeanstalkdQueue
	 */
	protected $queue;

	/**
	 * @var \Pheanstalk_Pheanstalk
	 */
	protected $pheanstalk;

	/**
	 * Set up dependencies
	 */
	public function setUp() {
		parent::setUp();
		$configurationManager = $this->objectManager->get('TYPO3\Flow\Configuration\ConfigurationManager');
		$settings = $configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'TYPO3.Jobqueue.Beanstalkd');
		if (!isset($settings['testing']['enabled']) || $settings['testing']['enabled'] !== TRUE) {
			$this->markTestSkipped('beanstalkd is not configured');
		}

		$queueName = 'Test-queue';
		$this->queue = new BeanstalkdQueue($queueName, $settings['testing']);

		$clientOptions = $settings['testing']['client'];
		$host = isset($clientOptions['host']) ? $clientOptions['host'] : '127.0.0.1';
		$port = isset($clientOptions['port']) ? $clientOptions['port'] : '11300';
		$this->pheanstalk = new \Pheanstalk_Pheanstalk($host, $port);

			// flush queue:
		try {
			while (true) {
				$job = $this->pheanstalk->peekDelayed($queueName);
				$this->pheanstalk->delete($job);
			}
		} catch (\Exception $e) {
		}
		try {
			while (true) {
				$job = $this->pheanstalk->peekBuried($queueName);
				$this->pheanstalk->delete($job);
			}
		} catch (\Exception $e) {
		}
		try {
			while (true) {
				$job = $this->pheanstalk->peekReady($queueName);
				$this->pheanstalk->delete($job);
			}
		} catch (\Exception $e) {
		}
	}

	/**
	 * @test
	 */
	public function publishAndWaitWithMessageWorks() {
		$message = new Message('Yeah, tell someone it works!');
		$this->queue->publish($message);

		$result = $this->queue->waitAndTake(1);
		$this->assertNotNull($result, 'wait should receive message');
		$this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');
	}

	/**
	 * @test
	 */
	public function waitForMessageTimesOut() {
		$result = $this->queue->waitAndTake(1);
		$this->assertNull($result, 'wait should return NULL after timeout');
	}

	/**
	 * @test
	 */
	public function peekReturnsNextMessagesIfQueueHasMessages() {
		$message = new Message('First message');
		$this->queue->publish($message);
		$message = new Message('Another message');
		$this->queue->publish($message);

		$results = $this->queue->peek(1);
		$this->assertEquals(1, count($results), 'peek should return a message');
		/** @var Message $result */
		$result = $results[0];
		$this->assertEquals('First message', $result->getPayload());
		$this->assertEquals(Message::STATE_PUBLISHED, $result->getState(), 'Message state should be published');

		$results = $this->queue->peek(1);
		$this->assertEquals(1, count($results), 'peek should return a message again');
		$result = $results[0];
		$this->assertEquals('First message', $result->getPayload(), 'second peek should return the same message again');
	}

	/**
	 * @test
	 */
	public function peekReturnsNullIfQueueHasNoMessage() {
		$result = $this->queue->peek();
		$this->assertEquals(array(), $result, 'peek should not return a message');
	}

	/**
	 * @test
	 */
	public function waitAndReserveWithFinishRemovesMessage() {
		$message = new Message('First message');
		$this->queue->publish($message);


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
	public function countReturnsZeroByDefault() {
		$this->assertSame(0, $this->queue->count());
	}

	/**
	 * @test
	 */
	public function countReturnsNumberOfReadyJobs() {
		$message1 = new Message('First message');
		$this->queue->publish($message1);

		$message2 = new Message('Second message');
		$this->queue->publish($message2);

		$this->assertSame(2, $this->queue->count());
	}

}