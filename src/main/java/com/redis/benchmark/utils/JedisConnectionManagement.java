package com.redis.benchmark.utils;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.MultiClusterClientConfig.ClusterConfig;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.MultiClusterClientConfig;
import redis.clients.jedis.providers.MultiClusterPooledConnectionProvider;

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
        MultiClusterClientConfig.Builder multiClusterJedisClientConfig;
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
                ClusterConfig[] clusterJedisClientConfigs = new ClusterConfig[hostAndPorts.size()];
                for (HostAndPort hostAndPort : hostAndPorts) {
                    clusterJedisClientConfigs[index] = new ClusterConfig(new HostAndPort(hostAndPort.getHost(), hostAndPort.getPort()), jedisClientConfig);
                    index++;
                }
                multiClusterJedisClientConfig = new MultiClusterClientConfig.Builder(clusterJedisClientConfigs);
                multiClusterJedisClientConfig.circuitBreakerSlidingWindowSize(Integer.parseInt(BenchmarkConfiguration.get().getConnectionCircuitBreakerSlidingWindowSize()));
                multiClusterJedisClientConfig.circuitBreakerSlidingWindowMinCalls(Integer.parseInt(BenchmarkConfiguration.get().getConnectionCircuitBreakerSlidingWindowMinCalls()));
                multiClusterJedisClientConfig.circuitBreakerFailureRateThreshold(Float.parseFloat(BenchmarkConfiguration.get().getConnectionCircuitBreakerFailureRateThreshold()));
                provider = new MultiClusterPooledConnectionProvider(multiClusterJedisClientConfig.build());

                // Optional post processor to register a custom callback
                //provider.setClusterFailoverPostProcessor(a -> System.out.println("\nActiveMultiClusterIndex=" + a));
                EmailAlertUtils emailAlertUtils = new EmailAlertUtils();
                provider.setClusterFailoverPostProcessor(emailAlertUtils);

                connectionManagement.unifiedJedis = new UnifiedJedis(provider);
            }
        } catch (Exception e) {
            System.err.println("------------------- Failed UnifiedJedis " + e.getMessage());
        }
    }

    public static UnifiedJedis getCommands() {
        if (!connectionCreated) {
            connectionManagement.createJedisConnection();
            connectionCreated = Boolean.TRUE;

            activeMultiClusterIndex = Integer.parseInt(provider.getClusterCircuitBreaker().getName().split(":")[1]);
            pidPath = System.getProperty("user.dir") + File.separator + "jedisPid" + File.separator;
            pidFile = pidPath + activeMultiClusterIndex + ".pid";

            try {
                PidFile.create(Path.of(pidFile), true, activeMultiClusterIndex);
                FileEventListener.FILE_EVENT_LISTENER.start(pidPath, 1000);
            } catch (Exception e) {
                System.err.println("------------------- Failed " + e.getMessage());
            }
        }
        return connectionManagement.unifiedJedis;
    }
}