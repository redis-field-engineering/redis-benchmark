package com.redis.benchmark.utils;

import redis.clients.jedis.HostAndPort;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

public final class BenchmarkConfiguration {
    private static final BenchmarkConfiguration configuration = new BenchmarkConfiguration();
    private Integer amountOfKeys = 0;

    private BenchmarkConfiguration() {
    }

    private Properties getProperties() {
        String path = System.getProperty("REDIS_BENCHMARK_CONFIG", String.valueOf(Paths.get("benchmark.properties")));

        Properties prop = new Properties();
        try {
            File f = new File(path);
            if (f.exists()) {
                prop.load(new FileInputStream(path));
            } else {
                System.err.println("\n\nERROR: Please create benchmark.properties and pass it as REDIS_BENCHMARK_CONFIG system property then execute the program!\n");
                String sample = "Sample benchmark.properties:\n" +
                        "##############################################\n" +
                        "#redis.connection=127.0.0.1:6379,127.0.0.1:6380\n" +
                        "# In this configuration, we've set a sliding window size of 10 and a failure rate threshold of 50%.\n" +
                        "# This means that a failover will be triggered if 5 out of any 10 calls to Redis fail.\n" +
                        "#redis.connection.circuit.breaker.sliding.window.size=10\n" +
                        "#redis.connection.circuit.breaker.sliding.window.min.calls=1\n" +
                        "#redis.connection.circuit.breaker.failure.rate.threshold=50.0f\n" +
                        "#redis.user=<Redis DB user or blank/default>\n" +
                        "#redis.password=<Redis DB password or blank if none>\n" +
                        "benchmark.key.amount=1000\n" +
                        "benchmark.key.data=USAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSA\n" +
                        "##############################################\n";
                System.out.println(sample);
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return prop;
    }

    private String getConnectionString() {
        Properties properties = getProperties();
        return properties.getProperty("redis.connection", "127.0.0.1:6379");
    }

    public String getConnectionCircuitBreakerSlidingWindowSize() {
        Properties properties = getProperties();
        return properties.getProperty("redis.connection.circuit.breaker.sliding.window.size", String.valueOf(10));
    }

    public String getConnectionCircuitBreakerSlidingWindowMinCalls() {
        Properties properties = getProperties();
        return properties.getProperty("redis.connection.circuit.breaker.sliding.window.min.calls", String.valueOf(1));
    }

    public String getConnectionCircuitBreakerFailureRateThreshold() {
        Properties properties = getProperties();
        return properties.getProperty("redis.connection.circuit.breaker.failure.rate.threshold", String.valueOf(50.0f));
    }

    public String getRedisUser() {
        Properties properties = getProperties();
        return properties.getProperty("redis.user", "default");
    }

    public String getRedisPassword() {
        Properties properties = getProperties();
        return properties.getProperty("redis.password", "");
    }

    String getKeyContentData() {
        Properties properties = getProperties();
        return properties.getProperty("benchmark.key.data", "USAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSAUSA");
    }

    public Integer getAmountOfKeys() {
        if (amountOfKeys > 0) {
            return amountOfKeys;
        }
        Properties properties = getProperties();
        amountOfKeys = Integer.parseInt(properties.getProperty("benchmark.key.amount", String.valueOf(1000)));
        return amountOfKeys;
    }

    public Set<HostAndPort> getRedisHostAndPorts() {
        String redisConnection = getConnectionString();
        Set<HostAndPort> hostAndPorts = new LinkedHashSet<>();
        if (redisConnection.contains(",")) {
            String[] redisConnections = redisConnection.split(",");
            for (String connection : redisConnections) {
                int lastColon = connection.lastIndexOf(":");
                String host = connection.substring(0, lastColon);
                int port = Integer.parseInt(connection.substring(lastColon + 1));
                hostAndPorts.add(new HostAndPort(host, port));
            }
        } else {
            int lastColon = redisConnection.lastIndexOf(":");
            String host = redisConnection.substring(0, lastColon);
            int port = Integer.parseInt(redisConnection.substring(lastColon + 1));
            hostAndPorts.add(new HostAndPort(host, port));
        }
        return hostAndPorts;
    }

    public static BenchmarkConfiguration get() {
        return configuration;
    }
}