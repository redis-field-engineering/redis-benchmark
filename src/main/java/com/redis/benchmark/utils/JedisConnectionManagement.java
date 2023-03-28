package com.redis.benchmark.utils;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.MultiClusterJedisClientConfig.ClusterJedisClientConfig;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.MultiClusterJedisClientConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisValidationException;
import redis.clients.jedis.providers.MultiClusterPooledConnectionProvider;
import java.util.Set;

public final class JedisConnectionManagement {
    private static final JedisConnectionManagement connectionManagement = new JedisConnectionManagement();
    private static final Boolean connectionCreated = false;
    private UnifiedJedis unifiedJedis;

    private JedisConnectionManagement() {
    }

    private void createJedisConnection() {
        MultiClusterPooledConnectionProvider provider = null;
        MultiClusterJedisClientConfig.Builder multiClusterJedisClientConfig;
        Set<HostAndPort> hostAndPorts = BenchmarkConfiguration.get().getRedisHostAndPorts();
        int index = 0;

        String user = BenchmarkConfiguration.get().getRedisUser();
        if (user != null && !user.isEmpty())
            user = BenchmarkConfiguration.get().getRedisUser();

        String password = BenchmarkConfiguration.get().getRedisPassword();
        if (password != null && !password.isEmpty())
            password = BenchmarkConfiguration.get().getRedisPassword();

        try {
            JedisClientConfig jedisClientConfig = DefaultJedisClientConfig.builder()
                    .user(user)
                    .password(password)
                    .clientName("RedisBenchmark")
                    .build();

            // Standalone
            if (hostAndPorts.size() == 1) {
                for (HostAndPort hostAndPort : hostAndPorts) {
                    unifiedJedis = new UnifiedJedis(new HostAndPort(hostAndPort.getHost(), hostAndPort.getPort()), jedisClientConfig);
                    break;
                }
                //unifiedJedis = new UnifiedJedis(HostAndPort.from(String.valueOf(hostAndPorts.stream().iterator().next())), jedisClientConfig);
            }
            // Multi cluster
            if (hostAndPorts.size() > 1) {
                ClusterJedisClientConfig[] clusterJedisClientConfigs = new ClusterJedisClientConfig[hostAndPorts.size()];
                for (HostAndPort hostAndPort : hostAndPorts) {
                    clusterJedisClientConfigs[index] = new ClusterJedisClientConfig(new HostAndPort(hostAndPort.getHost(), hostAndPort.getPort()), jedisClientConfig);
                    index++;
                }
                multiClusterJedisClientConfig = new MultiClusterJedisClientConfig.Builder(clusterJedisClientConfigs);
                multiClusterJedisClientConfig.circuitBreakerSlidingWindowSize(5);
                multiClusterJedisClientConfig.circuitBreakerSlidingWindowMinCalls(1);
                provider = new MultiClusterPooledConnectionProvider(multiClusterJedisClientConfig.build());

                if (provider.getConnection().ping())
                    provider.setActiveMultiClusterIndex(1);

                unifiedJedis = new UnifiedJedis(provider);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (provider != null && (e instanceof JedisValidationException || e instanceof JedisConnectionException)) {
                    provider.incrementActiveMultiClusterIndex();
                    unifiedJedis = new UnifiedJedis(provider);
            }
        }
    }

    public static JedisCommands getCommands() {
        if (!connectionCreated)
            connectionManagement.createJedisConnection();
        return connectionManagement.unifiedJedis;
    }
}