<?php
namespace TYPO3\Jobqueue\Beanstalkd\Tests\Functional\Queue;

/*                                                                        *
 * This script belongs to the FLOW3 package "Jobqueue.Beanstalkd".                *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        *
 * The TYPO3 project - inspiring people to share!                         *
 *                                                                        */

/**
 * Functional test for BeanstalkdQueue
 */
class BeanstalkdQueueTest extends \TYPO3\Flow\Tests\FunctionalTestCase {

	/**
	 * @var \TYPO3\Jobqueue\Beanstalkd\Queue\BeanstalkdQueue
	 */
	protected $queue;

	/**
	 * @var \Pheanstalk\Pheanstalk
	 */
	protected $pheanstalk;

	/**
	 * Set up dependencies
	 */
	public function setUp() {
		parent::setUp();
		$configurationManager = $this->objectManager->get('TYPO3\Flow\Configuration\ConfigurationManager');
		$settings = $configurationManager->getConfiguration(\TYPO3\Flow\Configuration\ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'TYPO3.Jobqueue.Beanstalkd');
		if (!isset($settings['testing']['enabled']) || $settings['testing']['enabled'] !== TRUE) {
			$this->markTestSkipped('beanstalkd is not configured');
		}

		$this->queue = new \TYPO3\Jobqueue\Beanstalkd\Queue\BeanstalkdQueue('Test queue', $settings['testing']);

		$clientOptions = $settings['testing']['client'];
		$host = isset($clientOptions['host']) ? $clientOptions['host'] : '127.0.0.1';
		$port = isset($clientOptions['port']) ? $clientOptions['port'] : '11300';
		$this->pheanstalk = new \Pheanstalk\Pheanstalk($host, $port);

			// flush queue:
		try {
			while (true) {
				$job = $this->pheanstalk->peekDelayed();
				$this->pheanstalk->delete($job);
			}
		} catch (\Exception $e) {
		}
		try {
			while (true) {
				$job = $this->pheanstalk->peekBuried();
				$this->pheanstalk->delete($job);
			}
		} catch (\Exception $e) {
		}
		try {
			while (true) {
				$job = $this->pheanstalk->peekReady();
				$this->pheanstalk->delete($job);
			}
		} catch (\Exception $e) {
		}
	}

	/**
	 * @test
	 */
	public function publishAndWaitWithMessageWorks() {
		$message = new \TYPO3\Jobqueue\Common\Queue\Message('Yeah, tell someone it works!');
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
		$message = new \TYPO3\Jobqueue\Common\Queue\Message('First message');
		$this->queue->publish($message);
		$message = new \TYPO3\Jobqueue\Common\Queue\Message('Another message');
		$this->queue->publish($message);

		$results = $this->queue->peek(1);
		$this->assertEquals(1, count($results), 'peek should return a message');
		$result = $results[0];
		$this->assertEquals('First message', $result->getPayload());
		$this->assertEquals(\TYPO3\Jobqueue\Common\Queue\Message::STATE_PUBLISHED, $result->getState(), 'Message state should be published');

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
		$message = new \TYPO3\Jobqueue\Common\Queue\Message('First message');
		$this->queue->publish($message);

		$result = $this->queue->waitAndReserve(1);
		$this->assertNotNull($result, 'waitAndReserve should receive message');
		$this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');

		$result = $this->queue->peek();
		$this->assertEquals(array(), $result, 'no message should be present in queue');

		$finishResult = $this->queue->finish($message);
		$this->assertTrue($finishResult, 'message should be finished');
	}

}
?>