<?php
namespace Flowpack\JobQueue\Beanstalkd\Tests\Unit\Queue;

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
use Pheanstalk\Pheanstalk;
use Pheanstalk\PheanstalkInterface;
use Neos\Flow\Tests\UnitTestCase;

/**
 * Queue manager
 */
class BeanstalkdQueueTest extends UnitTestCase
{

    /**
     * @var BeanstalkdQueue
     */
    protected $beanstalkdQueue;

    /**
     * @var Pheanstalk|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $mockClient;

    public function setUp()
    {
        $this->beanstalkdQueue = new BeanstalkdQueue('TestQueue');

        $this->mockClient = $this->getMockBuilder(Pheanstalk::class)->disableOriginalConstructor()->getMock();
        $this->inject($this->beanstalkdQueue, 'client', $this->mockClient);
    }

    /**
     * @test
     */
    public function submitPutsEncodedMessageIntoTube()
    {
        $messageData = [
            'identifier' => 'someIdentifier',
            'payload' => 'somePayload'
        ];
        $encodedMessage = json_encode($messageData);

        $this->mockClient->expects($this->once())->method('putInTube')->with('TestQueue', $encodedMessage);
        $this->beanstalkdQueue->submit($messageData);
    }

    /**
     * @test
     */
    public function submitRespectsPriorityOption()
    {
        $somePriority = 1337;
        $this->mockClient->expects($this->once())->method('putInTube')->with(new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), $somePriority);
        $this->beanstalkdQueue->submit('somePayload', ['priority' => $somePriority]);
    }

    /**
     * @test
     */
    public function submitSetsDefaultPriorityIfNotSpecified()
    {
        $this->mockClient->expects($this->once())->method('putInTube')->with(new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), PheanstalkInterface::DEFAULT_PRIORITY);
        $this->beanstalkdQueue->submit('somePayload');
    }

    /**
     * @test
     */
    public function submitRespectsDelayOption()
    {
        $someDelay = 42;
        $this->mockClient->expects($this->once())->method('putInTube')->with(new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), $someDelay);
        $this->beanstalkdQueue->submit('somePayload', ['delay' => $someDelay]);
    }

    /**
     * @test
     */
    public function submitSetsDefaultDelayIfNotSpecified()
    {
        $this->mockClient->expects($this->once())->method('putInTube')->with(new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), PheanstalkInterface::DEFAULT_DELAY);
        $this->beanstalkdQueue->submit('somePayload');
    }


    /**
     * @test
     */
    public function submitRespectsTtrOption()
    {
        $someTtr = 123;
        $this->mockClient->expects($this->once())->method('putInTube')->with(new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), $someTtr);
        $this->beanstalkdQueue->submit('somePayload', ['ttr' => $someTtr]);
    }

    /**
     * @test
     */
    public function submitSetsDefaultTtrIfNotSpecified()
    {
        $this->mockClient->expects($this->once())->method('putInTube')->with(new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), new \PHPUnit_Framework_Constraint_IsAnything(), PheanstalkInterface::DEFAULT_TTR);
        $this->beanstalkdQueue->submit('somePayload');
    }
}