<?php
declare(strict_types=1);

use React\EventLoop\Factory;
use React\Filesystem\Filesystem;
use React\Filesystem\Node\NodeInterface;
use WyriHaximus\React\Filesystem\S3\S3Adapter;

require 'vendor/autoload.php';

try {
    $loop = Factory::create();
    $loop->futureTick(static function () use ($loop) {
        $adapter = new S3Adapter($loop, [
            'credentials' => [
                'key' => 'KEY',
                'secret' => 'SECRET',
            ],
            'region' => 'REGION',
            'version' => 'latest',
        ], 'BUCKET');
        $stream = Filesystem::createFromAdapter($adapter)->dir('')->lsStreaming();
        $stream->on('data', static function (NodeInterface $node) {
            echo $node->getPath(), PHP_EOL;
        });
    });

    $loop->run();
} catch (Exception $e) {
    /** @noinspection UnusedFunctionResultInspection */
    var_export($e->getMessage());
}
