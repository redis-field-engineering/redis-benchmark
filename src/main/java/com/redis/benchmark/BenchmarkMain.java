package com.redis.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkMain {
    public static void main(String[] args) throws RunnerException {
        System.out.println("Starting Redis benchmark...");

        Options options = new OptionsBuilder()
                .include(RedisBenchmark.class.getSimpleName())
                .output("redis-benchmark.log")
                .forks(0)
                .build();
        new Runner(options).run();
    }
}
