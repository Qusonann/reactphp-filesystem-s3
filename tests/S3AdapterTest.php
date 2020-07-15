<?php
declare(strict_types=1);

namespace WyriHaximus\React\Tests\Filesystem\S3;


use PHPUnit\Framework\TestCase;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use WyriHaximus\React\Filesystem\S3\S3Adapter;

class S3AdapterTest extends TestCase
{
    private $adapter;

    protected function setUp(): void
    {
        parent::setUp();
        $loop = $this->createMock(LoopInterface::class);
        $this->adapter = new S3Adapter($loop, [
            'region' => 'us-west-2',
            'version' => '2006-03-01',
        ]);
    }

    public function test_construct(): void
    {
        $call = $this->adapter->stat("test");
        self::assertInstanceOf(PromiseInterface::class, $call);
    }
}