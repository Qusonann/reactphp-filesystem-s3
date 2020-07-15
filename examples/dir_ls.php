<?php
declare(strict_types=1);

use React\EventLoop\Factory;
use React\Filesystem\Filesystem;
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
        $filesystem = Filesystem::createFromAdapter($adapter);
        $filesystem->dir('')->ls()->then(static function (SplObjectStorage $ls) {
            foreach ($ls as $node) {
                echo $node->getPath(), PHP_EOL;
            }
            echo 'Found ', $ls->count(), ' nodes', PHP_EOL;
        }, static function ($e) {
            echo $e->getMessage(), PHP_EOL;
        });
    });

    $loop->run();
} catch (Exception $e) {
    /** @noinspection UnusedFunctionResultInspection */
    var_export($e->getMessage());
}
