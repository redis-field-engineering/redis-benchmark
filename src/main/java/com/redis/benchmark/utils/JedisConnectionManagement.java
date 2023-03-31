package com.redis.benchmark.utils;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.MultiClusterJedisClientConfig.ClusterJedisClientConfig;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.MultiClusterJedisClientConfig;
import redis.clients.jedis.providers.MultiClusterPooledConnectionProvider;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;

public final class JedisConnectionManagement {
    private static final JedisConnectionManagement connectionManagement = new JedisConnectionManagement();
    private static Boolean connectionCreated = false;
    private UnifiedJedis unifiedJedis;
    public static MultiClusterPooledConnectionProvider provider;
    public static int activeMultiClusterIndex;
    public static String pidPath;
    public static String pidFile;


    private JedisConnectionManagement() {
    }

    private void createJedisConnection() {
        MultiClusterJedisClientConfig.Builder multiClusterJedisClientConfig;
        Set<HostAndPort> hostAndPorts = BenchmarkConfiguration.get().getRedisHostAndPorts();
        int index = 0;

        try {
            JedisClientConfig jedisClientConfig = DefaultJedisClientConfig.builder()
                    .user(BenchmarkConfiguration.get().getRedisUser())
                    .password(BenchmarkConfiguration.get().getRedisPassword())
                    .clientName("RedisBenchmark")
                    .build();

            // Standalone
            if (hostAndPorts.size() == 1) {
                for (HostAndPort hostAndPort : hostAndPorts) {
                    connectionManagement.unifiedJedis = new UnifiedJedis(new HostAndPort(hostAndPort.getHost(), hostAndPort.getPort()), jedisClientConfig);
                    break;
                }
            }
            // Multi cluster
            if (hostAndPorts.size() > 1) {
                ClusterJedisClientConfig[] clusterJedisClientConfigs = new ClusterJedisClientConfig[hostAndPorts.size()];
                for (HostAndPort hostAndPort : hostAndPorts) {
                    clusterJedisClientConfigs[index] = new ClusterJedisClientConfig(new HostAndPort(hostAndPort.getHost(), hostAndPort.getPort()), jedisClientConfig);
                    index++;
                }
                multiClusterJedisClientConfig = new MultiClusterJedisClientConfig.Builder(clusterJedisClientConfigs);
                multiClusterJedisClientConfig.circuitBreakerSlidingWindowSize(Integer.parseInt(BenchmarkConfiguration.get().getConnectionCircuitBreakerSlidingWindowSize()));
                multiClusterJedisClientConfig.circuitBreakerSlidingWindowMinCalls(Integer.parseInt(BenchmarkConfiguration.get().getConnectionCircuitBreakerSlidingWindowMinCalls()));
                multiClusterJedisClientConfig.circuitBreakerFailureRateThreshold(Float.parseFloat(BenchmarkConfiguration.get().getConnectionCircuitBreakerFailureRateThreshold()));
                provider = new MultiClusterPooledConnectionProvider(multiClusterJedisClientConfig.build());

                connectionManagement.unifiedJedis = new UnifiedJedis(provider);

                activeMultiClusterIndex = Integer.parseInt(provider.getClusterCircuitBreaker().getName().split(":")[1]);
                pidPath = System.getProperty("user.dir") + File.separator + "jedisPid" + File.separator;
                pidFile = pidPath + activeMultiClusterIndex + ".pid";

                PidFile.create(Path.of(pidFile), true, activeMultiClusterIndex);

                FileEventListener.FILE_EVENT_LISTENER.start(pidPath, 1000);
            }
        } catch (Exception e) {
            System.err.println("------------------- Failed UnifiedJedis " + e.getMessage());
        }
    }

    public static JedisCommands getCommands() {
        if (!connectionCreated) {
            connectionManagement.createJedisConnection();
            connectionCreated = Boolean.TRUE;
        }
        return connectionManagement.unifiedJedis;
    }
}