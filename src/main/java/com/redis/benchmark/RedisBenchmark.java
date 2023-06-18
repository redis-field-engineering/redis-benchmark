package com.redis.benchmark;

import com.redis.benchmark.utils.BenchmarkConfiguration;
import com.redis.benchmark.utils.JedisConnectionManagement;
import com.redis.benchmark.utils.Util;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.TearDown;
import redis.clients.jedis.UnifiedJedis;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Warmup(iterations = 1)
@Threads(1)
@State(Scope.Benchmark)
@Measurement(iterations = 1, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RedisBenchmark {
    private static UnifiedJedis jedisCommands;
    private static Integer jedisGetCount = 0;
    private static Integer jedisSetCount = 0;

    @Setup(Level.Trial)
    public void setup() {
        System.out.println("\n------------------- Setup");

        Util.createOneMillionStringKeys();

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
            System.err.println("\n------------------- Failed GET Command\n" + e.getMessage());
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
            System.err.println("\n------------------- Failed SET Command\n" + e.getMessage());
        }
        return result;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        System.out.println("\n------------------- TearDown");
    }
}