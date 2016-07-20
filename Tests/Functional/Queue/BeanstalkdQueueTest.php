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

use Flowpack\JobQueue\Beanstalkd\Queue\BeanstalkdQueue;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Flowpack\JobQueue\Common\Tests\Functional\AbstractQueueTest;
use Pheanstalk\Pheanstalk;

/**
 * Functional test for BeanstalkdQueue
 */
class BeanstalkdQueueTest extends AbstractQueueTest
{

    /**
     * @inheritdoc
     */
    protected function getQueue()
    {
        return new BeanstalkdQueue('Test-queue', $this->queueSettings);
    }
}