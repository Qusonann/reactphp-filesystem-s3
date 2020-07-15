<?php

namespace WyriHaximus\React\Filesystem\S3;

use Aws\Result;
use Aws\S3\S3Client;
use Aws\Sdk;
use Exception;
use GuzzleHttp\HandlerStack;
use InvalidArgumentException;
use React\EventLoop\LoopInterface;
use React\EventLoop\Timer\Timer;
use React\Filesystem\AdapterInterface;
use React\Filesystem\FilesystemInterface;
use React\Filesystem\Node\NodeInterface;
use React\Filesystem\ObjectStream;
use React\Promise\Deferred;
use React\Promise\FulfilledPromise;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use React\Promise\RejectedPromise;
use React\Stream\WritableStreamInterface;
use WyriHaximus\React\GuzzlePsr7\HttpClientAdapter;
use function GuzzleHttp\Promise\queue;
use function React\Promise\resolve;

class S3Adapter implements AdapterInterface
{
    /**
     * @var LoopInterface
     */
    protected $loop;

    /**
     * @var
     */
    protected $queueTimer;

    /**
     * @var S3Client
     */
    protected $s3Client;

    /**
     * @var string
     */
    protected $bucket;

    /**
     * @var FilesystemInterface
     */
    protected $filesystem;

    /**
     * @param LoopInterface $loop
     * @param array $options
     * @param string $bucket
     */
    public function __construct(LoopInterface $loop, $options = [], $bucket = '')
    {
        $this->loop = $loop;
        if (!isset($options['http_handler']) || !($options['http_handler'] instanceof HandlerStack)) {
            $options['http_handler'] = HandlerStack::create(new HttpClientAdapter($loop));
        }
        $this->s3Client = (new Sdk($options))->createS3();
        $this->bucket = $bucket;
    }

    /**
     * @param FilesystemInterface $filesystem
     */
    public function setFilesystem(FilesystemInterface $filesystem)
    {
        $this->filesystem = $filesystem;
    }

    /**
     * @return LoopInterface
     */
    public function getLoop()
    {
        return $this->loop;
    }

    /**
     * @param string $function
     * @param array $args
     * @param int $errorResultCode
     * @return Promise
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
            queue()->run();

            /*if ($this->invoker->isEmpty()) {
                $timer->cancel();
            }*/
        });
    }

    /**
     * @param string $path
     * @param $mode
     * @return PromiseInterface
     */
    public function mkdir($path, $mode = self::CREATION_MODE)
    {
        return new FulfilledPromise();
    }

    /**
     * @param string $path
     * @return PromiseInterface
     */
    public function rmdir($path)
    {
        // TODO: Implement rmdir() method.
    }

    /**
     * @param string $filename
     * @return PromiseInterface
     */
    public function unlink($filename)
    {
        return $this->callFilesystem('deleteObject', [
            'Bucket' => $this->bucket,
            'Key' => $filename,
        ])->then(static function () {
            return new FulfilledPromise();
        }, static function () {
            return new RejectedPromise();
        });
    }

    /**
     * @param string $path
     * @param int $mode
     * @return PromiseInterface
     */
    public function chmod($path, $mode)
    {
        // TODO: Implement chmod() method.
    }

    /**
     * @param string $path
     * @param int $uid
     * @param int $gid
     * @return PromiseInterface
     */
    public function chown($path, $uid, $gid)
    {
        return new RejectedPromise(new Exception('No implemented, not intended for use'));
    }

    /**
     * @param string $filename
     * @return PromiseInterface
     */
    public function stat($filename)
    {
        if (substr($filename, -1) === NodeInterface::DS) {
            return new FulfilledPromise([]);
        }

        return $this->callFilesystem('headObject', [
            'Bucket' => $this->bucket,
            'Key' => $filename,
        ])->then(function (Result $result) {
            if ($result->toArray()['@metadata']['statusCode'] !== 200) {
                return new RejectedPromise();
            }

            return new FulfilledPromise([
                'size' => $result->get('ContentLength'),
                'atime' => $result->get('LastModified')->format('U'),
                'ctime' => $result->get('LastModified')->format('U'),
                'mtime' => $result->get('LastModified')->format('U'),
            ]);
        });
    }

    /**
     * @param string $path
     * @return ObjectStream
     */
    public function ls($path)
    {
        $stream = new ObjectStream();

        $this->callFilesystem('listObjects', [
            'Bucket' => $this->bucket,
            'Delimiter' => '/',
            'Prefix' => $path,
        ])->then(function ($ls) use ($stream) {
            $this->processLsContents($ls->toArray(), $stream);
        });

        return $stream;
    }

    protected function processLsContents($array, $stream)
    {
        if (isset($array['Contents'])) {
            foreach ($array['Contents'] as $file) {
                $stream->emit('data', [
                    $this->filesystem->file($file['Key']),
                ]);
            }
        }

        if (isset($array['CommonPrefixes'])) {
            foreach ($array['CommonPrefixes'] as $file) {
                $stream->emit('data', [
                    $this->filesystem->dir($file['Prefix']),
                ]);
            }
        }

        $stream->close();
    }

    /**
     * @param string $path
     * @param $mode
     * @return PromiseInterface
     */
    public function touch($path, $mode = self::CREATION_MODE)
    {
        return $this->openWrite($path)->then(function (WritableStreamInterface $stream) {
            $deferred = new Deferred();

            $stream->on('close', function () use ($deferred) {
                $deferred->resolve();
            });
            $stream->end('');

            return $deferred->promise();
        });
    }

    /**
     * @param string $path
     * @param string $flags
     * @param $mode
     * @return PromiseInterface
     */
    public function open($path, $flags, $mode = self::CREATION_MODE)
    {
        if (strpos($flags, 'r') !== false) {
            return $this->openRead($path);
        }

        if (strpos($flags, 'w') !== false) {
            return $this->openWrite($path);
        }

        throw new InvalidArgumentException('Open must be used with read or write flag');
    }

    protected function openRead($path)
    {
        return $this->callFilesystem('getObject', [
            'Bucket' => $this->bucket,
            'Key' => $path,
        ])->then(function (Result $result) {
            return resolve($result['Body']);
        });
    }

    protected function openWrite($path)
    {
        $sink = new BufferedSink();
        $sink->promise()->then(function ($body) use ($path) {
            return $this->callFilesystem('putObject', [
                'Body' => $body,
                'ContentLength' => strlen($body),
                'Bucket' => $this->bucket,
                'Key' => $path,
            ]);
        })->then(function ($result) {
            return resolve($result['Body']);
        });
        return resolve($sink);
    }

    /**
     * @param resource $fd
     * @return PromiseInterface
     */
    public function close($fd)
    {
        return new RejectedPromise(new Exception('No implemented, not intended for use'));
    }

    /**
     * @param $fileDescriptor
     * @param int $length
     * @param int $offset
     * @return PromiseInterface
     */
    public function read($fileDescriptor, $length, $offset)
    {
        return new RejectedPromise(new Exception('No implemented, not intended for use'));
    }

    /**
     * @param $fileDescriptor
     * @param string $data
     * @param int $length
     * @param int $offset
     * @return PromiseInterface
     */
    public function write($fileDescriptor, $data, $length, $offset)
    {
        return new RejectedPromise(new Exception('No implemented, not intended for use'));
    }

    /**
     * @param string $fromPath
     * @param string $toPath
     * @return PromiseInterface
     */
    public function rename($fromPath, $toPath)
    {
        return $this->callFilesystem('copyObject', [
            'Bucket' => $this->bucket,
            'Key' => $toPath,
            'CopySource' => $this->bucket . '/' . $fromPath,
        ])->then(function () use ($fromPath) {
            return $this->unlink($fromPath);
        })->then(function () {
            return new FulfilledPromise();
        }, function () {
            return new RejectedPromise();
        });
    }

    public static function isSupported()
    {
        return true;
    }

    public function getFilesystem()
    {
        return $this->filesystem;
    }

    public function lsStream($path)
    {
        // TODO: Implement lsStream() method.
    }

    public function getContents($path, $offset = 0, $length = null)
    {
        // TODO: Implement getContents() method.
    }

    public function putContents($path, $content)
    {
        // TODO: Implement putContents() method.
    }

    public function appendContents($path, $content)
    {
        // TODO: Implement appendContents() method.
    }

    public function readlink($path)
    {
        // TODO: Implement readlink() method.
    }

    public function symlink($fromPath, $toPath)
    {
        // TODO: Implement symlink() method.
    }

    public function detectType($path)
    {
        // TODO: Implement detectType() method.
    }
}
