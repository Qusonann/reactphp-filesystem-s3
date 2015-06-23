<?php

namespace WyriHaximus\React\Filesystem\S3;

use Aws\S3\S3Client;
use Aws\Sdk;
use GuzzleHttp\HandlerStack;
use React\EventLoop\LoopInterface;
use React\EventLoop\Timer\Timer;
use React\Filesystem\AdapterInterface;
use React\Filesystem\CallInvokerInterface;
use React\Filesystem\Node\Directory;
use React\Filesystem\Node\File;
use React\Filesystem\PooledInvoker;
use React\Promise\Deferred;
use WyriHaximus\React\GuzzlePsr7\HttpClientAdapter;

class S3Adapter implements AdapterInterface
{
    protected $loop;
    protected $queueTimer;

    /**
     * @var S3Client
     */
    protected $s3Client;

    protected $bucket;

    protected $invoker;

    public function __construct(LoopInterface $loop, $options = [], $bucket = '')
    {
        $this->loop = $loop;
        $options['http_handler'] = HandlerStack::create(new HttpClientAdapter($loop));
        $this->s3Client = (new Sdk($options))->createS3();
        $this->bucket = $bucket;
        $this->invoker = new PooledInvoker($this);
    }

    /**
     * @return LoopInterface
     */
    public function getLoop()
    {
        return $this->loop;
    }

    /**
     * @param CallInvokerInterface $invoker
     * @return void
     */
    public function setInvoker(CallInvokerInterface $invoker)
    {
        $this->invoker = $invoker;
    }

    /**
     * @param string $function
     * @param array $args
     * @param int $errorResultCode
     * @return \React\Promise\Promise
     */
    public function callFilesystem($function, $args, $errorResultCode = -1)
    {
        $this->startQueue();

        $deferred = new Deferred();
        $this->s3Client->{$function . 'Async'}($args)->then(function ($result) use ($deferred) {
            $deferred->resolve($result);
        }, function ($error) use ($deferred) {
            $deferred->reject($error);
        });
        return $deferred->promise();
    }

    protected function startQueue()
    {
        if ($this->queueTimer instanceof Timer && $this->loop->isTimerActive($this->queueTimer)) {
            return;
        }

        $this->queueTimer = $this->loop->addPeriodicTimer(HttpClientAdapter::QUEUE_TIMER_INTERVAL, function (Timer $timer) {
            \GuzzleHttp\Promise\queue()->run();

            if ($this->invoker->isEmpty()) {
                $timer->cancel();
            }
        });
    }

    /**
     * @param string $path
     * @param $mode
     * @return \React\Promise\PromiseInterface
     */
    public function mkdir($path, $mode = self::CREATION_MODE)
    {
        // TODO: Implement mkdir() method.
    }

    /**
     * @param string $path
     * @return \React\Promise\PromiseInterface
     */
    public function rmdir($path)
    {
        // TODO: Implement rmdir() method.
    }

    /**
     * @param string $filename
     * @return \React\Promise\PromiseInterface
     */
    public function unlink($filename)
    {
        // TODO: Implement unlink() method.
    }

    /**
     * @param string $path
     * @param int $mode
     * @return \React\Promise\PromiseInterface
     */
    public function chmod($path, $mode)
    {
        // TODO: Implement chmod() method.
    }

    /**
     * @param string $path
     * @param int $uid
     * @param int $gid
     * @return \React\Promise\PromiseInterface
     */
    public function chown($path, $uid, $gid)
    {
        // TODO: Implement chown() method.
    }

    /**
     * @param string $filename
     * @return \React\Promise\PromiseInterface
     */
    public function stat($filename)
    {
        // TODO: Implement stat() method.
    }

    /**
     * @param string $path
     * @param int $flags
     * @return \React\Promise\PromiseInterface
     */
    public function ls($path, $flags = EIO_READDIR_DIRS_FIRST)
    {
        return $this->invoker->invokeCall('listObjects', [
            'Bucket' => $this->bucket,
            'Delimiter' => '/',
            'Prefix' => $path,
        ])->then(function ($ls) {
            $deferred = new Deferred();
            $this->loop->futureTick(function () use ($ls, $deferred) {
                $this->processLsContents($ls->toArray(), $deferred);
            });
            return $deferred->promise();
        });
    }

    protected function processLsContents($array, $deferred)
    {
        $list = new \SplObjectStorage();

        if (isset($array['Contents'])) {
            foreach ($array['Contents'] as $file) {
                $node = new File($file['Key'], $this);
                $deferred->progress($node);
                $list->attach($node);
            }
        }

        if (isset($array['CommonPrefixes'])) {
            foreach ($array['CommonPrefixes'] as $file) {
                $node = new Directory($file['Prefix'], $this);
                $deferred->progress($node);
                $list->attach($node);
            }
        }

        $deferred->resolve($list);
    }

    /**
     * @param string $path
     * @param $mode
     * @return \React\Promise\PromiseInterface
     */
    public function touch($path, $mode = self::CREATION_MODE)
    {
        // TODO: Implement touch() method.
    }

    /**
     * @param string $path
     * @param string $flags
     * @param $mode
     * @return \React\Promise\PromiseInterface
     */
    public function open($path, $flags, $mode = self::CREATION_MODE)
    {
        // TODO: Implement open() method.
    }

    /**
     * @param resource $fd
     * @return \React\Promise\PromiseInterface
     */
    public function close($fd)
    {
        // TODO: Implement close() method.
    }
}