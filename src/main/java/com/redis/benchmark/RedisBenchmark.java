package com.redis.benchmark;

import com.redis.benchmark.utils.BenchmarkConfiguration;
import com.redis.benchmark.utils.JedisConnectionManagement;
import com.redis.benchmark.utils.Util;
import org.openjdk.jmh.annotations.*;
import redis.clients.jedis.commands.JedisCommands;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Warmup(iterations = 1)
@Threads(1)
@State(Scope.Thread)
@Measurement(iterations = 1, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RedisBenchmark {
    private JedisCommands jedisCommands;
    private static Integer jedisGetCount = 0;
    private static Integer jedisSetCount = 0;

    @Setup
    public void setup() {
        Util.createOneMillionOfKeys();

        jedisCommands = JedisConnectionManagement.getCommands();
    }

    @Benchmark
    public String jedisSimpleGet() {
        if (jedisGetCount >= BenchmarkConfiguration.get().getAmountOfKeys()) {
            jedisGetCount = 0;
        }
        jedisGetCount++;
        String result = null;
        try {
            result = jedisCommands.get(String.format(Util.KeyPrefix, jedisGetCount));
        } catch (Exception e) {
            e.printStackTrace();
            jedisCommands = JedisConnectionManagement.getCommands();
        }
        return result;
    }

    @Benchmark
    public String jedisSimpleSet() {
        jedisSetCount++;
        String result = null;
        try {
            result = jedisCommands.set(String.format("JedisSetTest%s", jedisSetCount), jedisSetCount.toString());
        } catch (Exception e) {
            e.printStackTrace();
            jedisCommands = JedisConnectionManagement.getCommands();
        }
        return result;
    }
}