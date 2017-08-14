package redis.clients.jedis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.techwolf.RedisHystrixCommand;
import redis.clients.techwolf.TechwolfHystrixRedisWorker;
import redis.clients.techwolf.TechwolfJedisConfig;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.JedisClusterHashTagUtil;
import redis.clients.util.KeyMergeUtil;
import redis.clients.util.SafeEncoder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TechwolfHystrixJedisCluster extends TechwolfBinaryJedisCluster implements JedisCommands,
        MultiKeyJedisClusterCommands, JedisClusterScriptingCommands {

    public static enum Reset {
        SOFT, HARD
    }

    public TechwolfHystrixJedisCluster(TechwolfJedisConfig config) {
        super(config.getHostAndPortSet(),
                config.getConnectionTimeout(),
                config.getSoTimeout(),
                config.getMaxAttempts(),
                config.getPassword(),
                config.getClientName(),
                config.getPoolConfig(),
                config.isUseSlave()
        );
    }

    public TechwolfHystrixJedisCluster(HostAndPort node, int connectionTimeout, int soTimeout,
                                       int maxAttempts, String password, String clientName, final GenericObjectPoolConfig poolConfig) {
        super(Collections.singleton(node), connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public TechwolfHystrixJedisCluster(HostAndPort node, String password, String clientName, final GenericObjectPoolConfig poolConfig) {
        super(Collections.singleton(node), password, clientName, poolConfig);
    }

    @Override
    public String set(final String key, final String value) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler.write(), maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.set(key, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "set").execute();
    }

    @Override
    public String set(final String key, final String value, final String nxxx, final String expx,
                      final long time) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.set(key, value, nxxx, expx, time);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "set").execute();
    }


    @Override
    public String get(final String key) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler.read(), maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.get(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "get").execute();
    }

    private String getGroupName(String key) {
        return connectionHandler.getHostAndPortBySlot(JedisClusterCRC16.getSlot(key));
    }

    @Override
    public Boolean exists(final String key) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.exists(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "exists").execute();
    }

    @Override
    public Long exists(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.exists(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(keys[0]), "exists").execute();
    }

    @Override
    public Long persist(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.persist(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "persist").execute();
    }

    @Override
    public String type(final String key) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.type(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "type").execute();
    }

    @Override
    public Long expire(final String key, final int seconds) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.expire(key, seconds);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "expire").execute();
    }

    @Override
    public Long pexpire(final String key, final long milliseconds) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.pexpire(key, milliseconds);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "pexpire").execute();
    }

    @Override
    public Long expireAt(final String key, final long unixTime) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.expireAt(key, unixTime);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "expireAt").execute();
    }

    @Override
    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.pexpireAt(key, millisecondsTimestamp);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "pexpireAt").execute();
    }

    @Override
    public Long ttl(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.ttl(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "ttl").execute();
    }

    @Override
    public Long pttl(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.pttl(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "pttl").execute();
    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.setbit(key, offset, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "setbit").execute();
    }

    @Override
    public Boolean setbit(final String key, final long offset, final String value) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.setbit(key, offset, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "setbit").execute();
    }

    @Override
    public Boolean getbit(final String key, final long offset) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.getbit(key, offset);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "getbit").execute();
    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.setrange(key, offset, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "setrange").execute();
    }

    @Override
    public String getrange(final String key, final long startOffset, final long endOffset) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.getrange(key, startOffset, endOffset);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "getrange").execute();
    }

    @Override
    public String getSet(final String key, final String value) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.getSet(key, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "getSet").execute();
    }

    @Override
    public Long setnx(final String key, final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.setnx(key, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "setnx").execute();
    }

    @Override
    public String setex(final String key, final int seconds, final String value) {

        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.setex(key, seconds, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "setex").execute();
    }

    @Override
    public String psetex(final String key, final long milliseconds, final String value) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.psetex(key, milliseconds, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "psetex").execute();
    }

    @Override
    public Long decrBy(final String key, final long integer) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.decrBy(key, integer);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "decrBy").execute();
    }

    @Override
    public Long decr(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.decr(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "decr").execute();
    }

    @Override
    public Long incrBy(final String key, final long integer) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.incrBy(key, integer);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "incrBy").execute();
    }

    @Override
    public Double incrByFloat(final String key, final double value) {
        TechwolfHystrixRedisWorker<Double> worker = new TechwolfHystrixRedisWorker<Double>() {
            @Override
            public Double work() {
                return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
                    @Override
                    public Double execute(Jedis connection) {
                        return connection.incrByFloat(key, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Double>(worker, getGroupName(key), "incrByFloat").execute();
    }

    @Override
    public Long incr(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.incr(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "incr").execute();
    }

    @Override
    public Long append(final String key, final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.append(key, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "append").execute();
    }

    @Override
    public String substr(final String key, final int start, final int end) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.substr(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "substr").execute();
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.hset(key, field, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "hset").execute();
    }

    @Override
    public String hget(final String key, final String field) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.hget(key, field);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "hget").execute();
    }

    @Override
    public Long hsetnx(final String key, final String field, final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.hsetnx(key, field, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "hsetnx").execute();
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.hmset(key, hash);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "hmset").execute();
    }

    @Override
    public List<String> hmget(final String key, final String... fields) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.hmget(key, fields);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "hmget").execute();
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.hincrBy(key, field, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "hincrBy").execute();
    }

    @Override
    public Double hincrByFloat(final String key, final String field, final double value) {
        TechwolfHystrixRedisWorker<Double> worker = new TechwolfHystrixRedisWorker<Double>() {
            @Override
            public Double work() {
                return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
                    @Override
                    public Double execute(Jedis connection) {
                        return connection.hincrByFloat(key, field, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Double>(worker, getGroupName(key), "hincrByFloat").execute();
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.hexists(key, field);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "hexists").execute();
    }

    @Override
    public Long hdel(final String key, final String... field) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.hdel(key, field);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "hdel").execute();
    }

    @Override
    public Long hlen(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.hlen(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "hlen").execute();
    }

    @Override
    public Set<String> hkeys(final String key) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.hkeys(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "hkeys").execute();
    }

    @Override
    public List<String> hvals(final String key) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.hvals(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "hvals").execute();
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        TechwolfHystrixRedisWorker<Map<String, String>> worker = new TechwolfHystrixRedisWorker<Map<String, String>>() {
            @Override
            public Map<String, String> work() {
                return new TechwolfJedisClusterCommand<Map<String, String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Map<String, String> execute(Jedis connection) {
                        return connection.hgetAll(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Map<String, String>>(worker, getGroupName(key), "hgetAll").execute();
    }

    @Override
    public Long rpush(final String key, final String... string) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.rpush(key, string);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "rpush").execute();
    }

    @Override
    public Long lpush(final String key, final String... string) {

        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.lpush(key, string);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "lpush").execute();
    }

    @Override
    public Long llen(final String key) {

        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.llen(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "llen").execute();
    }

    @Override
    public List<String> lrange(final String key, final long start, final long end) {

        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.lrange(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "lrange").execute();
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.ltrim(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "ltrim").execute();
    }

    @Override
    public String lindex(final String key, final long index) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.lindex(key, index);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "lindex").execute();
    }

    @Override
    public String lset(final String key, final long index, final String value) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.lset(key, index, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "lset").execute();
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.lrem(key, count, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "lrem").execute();
    }

    @Override
    public String lpop(final String key) {

        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.lpop(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "lpop").execute();
    }

    @Override
    public String rpop(final String key) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.rpop(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "rpop").execute();
    }

    @Override
    public Long sadd(final String key, final String... member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.sadd(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "sadd").execute();
    }

    @Override
    public Set<String> smembers(final String key) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.smembers(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "smembers").execute();
    }

    @Override
    public Long srem(final String key, final String... member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.srem(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "srem").execute();
    }

    @Override
    public String spop(final String key) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.spop(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "spop").execute();
    }

    @Override
    public Set<String> spop(final String key, final long count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.spop(key, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "spop").execute();
    }

    @Override
    public Long scard(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.scard(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "scard").execute();
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.sismember(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "sismember").execute();
    }

    @Override
    public String srandmember(final String key) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.srandmember(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "srandmember").execute();
    }

    @Override
    public List<String> srandmember(final String key, final int count) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.srandmember(key, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "srandmember").execute();
    }

    @Override
    public Long strlen(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.strlen(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "strlen").execute();
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zadd(key, score, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zadd").execute();
    }

    @Override
    public Long zadd(final String key, final double score, final String member,
                     final ZAddParams params) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zadd(key, score, member, params);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zadd").execute();
    }

    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zadd(key, scoreMembers);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zadd").execute();
    }

    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers, final ZAddParams params) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zadd(key, scoreMembers, params);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zadd").execute();
    }

    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrange(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrange").execute();
    }

    @Override
    public Long zrem(final String key, final String... member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zrem(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zrem").execute();
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        TechwolfHystrixRedisWorker<Double> worker = new TechwolfHystrixRedisWorker<Double>() {
            @Override
            public Double work() {
                return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
                    @Override
                    public Double execute(Jedis connection) {
                        return connection.zincrby(key, score, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Double>(worker, getGroupName(key), "zincrby").execute();
    }

    @Override
    public Double zincrby(final String key, final double score, final String member,
                          final ZIncrByParams params) {
        TechwolfHystrixRedisWorker<Double> worker = new TechwolfHystrixRedisWorker<Double>() {
            @Override
            public Double work() {
                return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
                    @Override
                    public Double execute(Jedis connection) {
                        return connection.zincrby(key, score, member, params);
                    }
                }.run(key);

            }
        };
        return new RedisHystrixCommand<Double>(worker, getGroupName(key), "zincrby").execute();
    }

    @Override
    public Long zrank(final String key, final String member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zrank(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zrank").execute();
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zrevrank(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zrevrank").execute();
    }

    @Override
    public Set<String> zrevrange(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrange(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrange").execute();
    }

    @Override
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrangeWithScores(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrangeWithScores").execute();
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrevrangeWithScores(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrevrangeWithScores").execute();
    }

    @Override
    public Long zcard(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zcard(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zcard").execute();
    }

    @Override
    public Double zscore(final String key, final String member) {
        TechwolfHystrixRedisWorker<Double> worker = new TechwolfHystrixRedisWorker<Double>() {
            @Override
            public Double work() {
                return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
                    @Override
                    public Double execute(Jedis connection) {
                        return connection.zscore(key, member);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Double>(worker, getGroupName(key), "zscore").execute();
    }

    @Override
    public List<String> sort(final String key) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.sort(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "sort").execute();
    }

    @Override
    public List<String> sort(final String key, final SortingParams sortingParameters) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.sort(key, sortingParameters);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "sort").execute();
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zcount(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zcount").execute();
    }

    @Override
    public Long zcount(final String key, final String min, final String max) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zcount(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zcount").execute();
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrangeByScore(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrangeByScore").execute();
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrangeByScore(key, min, max);
                    }
                }.run(key);

            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrangeByScore").execute();
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrangeByScore(key, max, min);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrangeByScore").execute();
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max,
                                     final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrangeByScore(key, min, max, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrangeByScore").execute();
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrangeByScore(key, max, min);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrangeByScore").execute();
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max,
                                     final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrangeByScore(key, min, max, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrangeByScore").execute();
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min,
                                        final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrangeByScore(key, max, min, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrangeByScore").execute();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrangeByScoreWithScores(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrangeByScoreWithScores").execute();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrevrangeByScoreWithScores(key, max, min);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrevrangeByScoreWithScores").execute();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max,
                                              final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrangeByScoreWithScores(key, min, max, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrangeByScoreWithScores").execute();
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min,
                                        final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrangeByScore(key, max, min, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrangeByScore").execute();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrangeByScoreWithScores(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrangeByScoreWithScores").execute();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrevrangeByScoreWithScores(key, max, min);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrevrangeByScoreWithScores").execute();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max,
                                              final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrangeByScoreWithScores(key, min, max, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrangeByScoreWithScores").execute();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max,
                                                 final double min, final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrevrangeByScoreWithScores(key, max, min, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrevrangeByScoreWithScores").execute();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max,
                                                 final String min, final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<Tuple>> worker = new TechwolfHystrixRedisWorker<Set<Tuple>>() {
            @Override
            public Set<Tuple> work() {
                return new TechwolfJedisClusterCommand<Set<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<Tuple> execute(Jedis connection) {
                        return connection.zrevrangeByScoreWithScores(key, max, min, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<Tuple>>(worker, getGroupName(key), "zrevrangeByScoreWithScores").execute();
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zremrangeByRank(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zremrangeByRank").execute();
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zremrangeByScore(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zremrangeByScore").execute();
    }

    @Override
    public Long zremrangeByScore(final String key, final String start, final String end) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zremrangeByScore(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zremrangeByScore").execute();
    }

    @Override
    public Long zlexcount(final String key, final String min, final String max) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zlexcount(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zlexcount").execute();
    }

    @Override
    public Set<String> zrangeByLex(final String key, final String min, final String max) {


        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrangeByLex(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrangeByLex").execute();
    }

    @Override
    public Set<String> zrangeByLex(final String key, final String min, final String max,
                                   final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrangeByLex(key, min, max, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrangeByLex").execute();
    }

    @Override
    public Set<String> zrevrangeByLex(final String key, final String max, final String min) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrangeByLex(key, max, min);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrangeByLex").execute();
    }

    @Override
    public Set<String> zrevrangeByLex(final String key, final String max, final String min,
                                      final int offset, final int count) {
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.zrevrangeByLex(key, max, min, offset, count);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(key), "zrevrangeByLex").execute();
    }

    @Override
    public Long zremrangeByLex(final String key, final String min, final String max) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zremrangeByLex(key, min, max);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "zremrangeByLex").execute();
    }

    @Override
    public Long linsert(final String key, final LIST_POSITION where, final String pivot,
                        final String value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.linsert(key, where, pivot, value);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "linsert").execute();
    }

    @Override
    public Long lpushx(final String key, final String... string) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.lpushx(key, string);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "lpushx").execute();
    }

    @Override
    public Long rpushx(final String key, final String... string) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.rpushx(key, string);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "rpushx").execute();
    }

    @Override
    public Long del(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.del(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "del").execute();
    }

    @Override
    public String echo(final String string) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.echo(string);
                    }
                }.run(string);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(string), "echo").execute();
    }

    @Override
    public Long bitcount(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.bitcount(key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "bitcount").execute();
    }

    @Override
    public Long bitcount(final String key, final long start, final long end) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.bitcount(key, start, end);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "bitcount").execute();
    }

    @Override
    public ScanResult<String> scan(final String cursor, final ScanParams params) {

        String matchPattern = null;

        if (params == null || (matchPattern = params.match()) == null || matchPattern.isEmpty()) {
            throw new IllegalArgumentException(TechwolfHystrixJedisCluster.class.getSimpleName() + " only supports SCAN commands with non-empty MATCH patterns");
        }

        if (JedisClusterHashTagUtil.isClusterCompliantMatchPattern(matchPattern)) {

            return new TechwolfJedisClusterCommand<ScanResult<String>>(connectionHandler,
                    maxAttempts) {
                @Override
                public ScanResult<String> execute(Jedis connection) {
                    return connection.scan(cursor, params);
                }
            }.runBinary(SafeEncoder.encode(matchPattern));
        } else {
            throw new IllegalArgumentException(TechwolfHystrixJedisCluster.class.getSimpleName() + " only supports SCAN commands with MATCH patterns containing hash-tags ( curly-brackets enclosed strings )");
        }
    }

    @Override
    public Long bitpos(final String key, final boolean value) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.bitpos(key, value);
                    }
                }.run(key);

            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "bitpos").execute();
    }

    @Override
    public Long bitpos(final String key, final boolean value, final BitPosParams params) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.bitpos(key, value, params);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "bitpos").execute();
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(final String key, final String cursor) {
        TechwolfHystrixRedisWorker<ScanResult<Entry<String, String>>> worker = new TechwolfHystrixRedisWorker<ScanResult<Entry<String, String>>>() {
            @Override
            public ScanResult<Entry<String, String>> work() {
                return new TechwolfJedisClusterCommand<ScanResult<Entry<String, String>>>(connectionHandler,
                        maxAttempts) {
                    @Override
                    public ScanResult<Entry<String, String>> execute(Jedis connection) {
                        return connection.hscan(key, cursor);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<ScanResult<Entry<String, String>>>(worker, getGroupName(key), "hscan").execute();
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(final String key, final String cursor,
                                                   final ScanParams params) {
        TechwolfHystrixRedisWorker<ScanResult<Entry<String, String>>> worker = new TechwolfHystrixRedisWorker<ScanResult<Entry<String, String>>>() {
            @Override
            public ScanResult<Entry<String, String>> work() {
                return new TechwolfJedisClusterCommand<ScanResult<Entry<String, String>>>(connectionHandler,
                        maxAttempts) {
                    @Override
                    public ScanResult<Entry<String, String>> execute(Jedis connection) {
                        return connection.hscan(key, cursor, params);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<ScanResult<Entry<String, String>>>(worker, getGroupName(key), "hscan").execute();
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor) {
        TechwolfHystrixRedisWorker<ScanResult<String>> worker = new TechwolfHystrixRedisWorker<ScanResult<String>>() {
            @Override
            public ScanResult<String> work() {
                return new TechwolfJedisClusterCommand<ScanResult<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public ScanResult<String> execute(Jedis connection) {
                        return connection.sscan(key, cursor);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<ScanResult<String>>(worker, getGroupName(key), "sscan").execute();
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
        TechwolfHystrixRedisWorker<ScanResult<String>> worker = new TechwolfHystrixRedisWorker<ScanResult<String>>() {
            @Override
            public ScanResult<String> work() {
                return new TechwolfJedisClusterCommand<ScanResult<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public ScanResult<String> execute(Jedis connection) {
                        return connection.sscan(key, cursor, params);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<ScanResult<String>>(worker, getGroupName(key), "sscan").execute();
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final String cursor) {
        TechwolfHystrixRedisWorker<ScanResult<Tuple>> worker = new TechwolfHystrixRedisWorker<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> work() {
                return new TechwolfJedisClusterCommand<ScanResult<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public ScanResult<Tuple> execute(Jedis connection) {
                        return connection.zscan(key, cursor);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<ScanResult<Tuple>>(worker, getGroupName(key), "zscan").execute();
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final String cursor, final ScanParams params) {
        TechwolfHystrixRedisWorker<ScanResult<Tuple>> worker = new TechwolfHystrixRedisWorker<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> work() {
                return new TechwolfJedisClusterCommand<ScanResult<Tuple>>(connectionHandler, maxAttempts) {
                    @Override
                    public ScanResult<Tuple> execute(Jedis connection) {
                        return connection.zscan(key, cursor, params);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<ScanResult<Tuple>>(worker, getGroupName(key), "zscan").execute();
    }

    @Override
    public Long pfadd(final String key, final String... elements) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.pfadd(key, elements);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "pfadd").execute();
    }

    @Override
    public long pfcount(final String key) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.pfcount(key);
                    }
                }.run(key);

            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "pfcount").execute();
    }

    @Override
    public List<String> blpop(final int timeout, final String key) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.blpop(timeout, key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "blpop").execute();
    }

    @Override
    public List<String> brpop(final int timeout, final String key) {
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.brpop(timeout, key);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(key), "brpop").execute();
    }

    @Override
    public Long del(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.del(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(keys[0]), "del").execute();
    }

    @Override
    public List<String> blpop(final int timeout, final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.blpop(timeout, keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(keys[0]), "blpop").execute();

    }

    @Override
    public List<String> brpop(final int timeout, final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.brpop(timeout, keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(keys[0]), "brpop").execute();
    }

    @Override
    public List<String> mget(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<List<String>> worker = new TechwolfHystrixRedisWorker<List<String>>() {
            @Override
            public List<String> work() {
                return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<String> execute(Jedis connection) {
                        return connection.mget(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<List<String>>(worker, getGroupName(keys[0]), "mget").execute();
    }

    @Override
    public String mset(final String... keysvalues) {
        final String[] keys = new String[keysvalues.length / 2];

        for (int keyIdx = 0; keyIdx < keys.length; keyIdx++) {
            keys[keyIdx] = keysvalues[keyIdx * 2];
        }
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.mset(keysvalues);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(keys[0]), "mset").execute();
    }

    @Override
    public Long msetnx(final String... keysvalues) {
        final String[] keys = new String[keysvalues.length / 2];

        for (int keyIdx = 0; keyIdx < keys.length; keyIdx++) {
            keys[keyIdx] = keysvalues[keyIdx * 2];
        }
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.msetnx(keysvalues);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(keys[0]), "msetnx").execute();
    }

    @Override
    public String rename(final String oldkey, final String newkey) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.rename(oldkey, newkey);
                    }
                }.run(2, oldkey, newkey);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(oldkey), "rename").execute();
    }

    @Override
    public Long renamenx(final String oldkey, final String newkey) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.renamenx(oldkey, newkey);
                    }
                }.run(2, oldkey, newkey);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(oldkey), "renamenx").execute();
    }

    @Override
    public String rpoplpush(final String srckey, final String dstkey) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.rpoplpush(srckey, dstkey);
                    }
                }.run(2, srckey, dstkey);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(srckey), "rpoplpush").execute();
    }

    @Override
    public Set<String> sdiff(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.sdiff(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(keys[0]), "sdiff").execute();
    }

    @Override
    public Long sdiffstore(final String dstkey, final String... keys) {
        final String[] mergedKeys = KeyMergeUtil.merge(dstkey, keys);
        if (mergedKeys == null || mergedKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.sdiffstore(dstkey, keys);
                    }
                }.run(mergedKeys.length, mergedKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(mergedKeys[0]), "sdiffstore").execute();
    }

    @Override
    public Set<String> sinter(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.sinter(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(keys[0]), "sinter").execute();
    }

    @Override
    public Long sinterstore(final String dstkey, final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        final String[] mergedKeys = KeyMergeUtil.merge(dstkey, keys);
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.sinterstore(dstkey, keys);
                    }
                }.run(mergedKeys.length, mergedKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(keys[0]), "sinterstore").execute();
    }

    @Override
    public Long smove(final String srckey, final String dstkey, final String member) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.smove(srckey, dstkey, member);
                    }
                }.run(2, srckey, dstkey);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(srckey), "smove").execute();
    }

    @Override
    public Long sort(final String key, final SortingParams sortingParameters, final String dstkey) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.sort(key, sortingParameters, dstkey);
                    }
                }.run(2, key, dstkey);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "sort").execute();
    }

    @Override
    public Long sort(final String key, final String dstkey) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.sort(key, dstkey);
                    }
                }.run(2, key, dstkey);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(key), "sort").execute();
    }

    @Override
    public Set<String> sunion(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Set<String>> worker = new TechwolfHystrixRedisWorker<Set<String>>() {
            @Override
            public Set<String> work() {
                return new TechwolfJedisClusterCommand<Set<String>>(connectionHandler, maxAttempts) {
                    @Override
                    public Set<String> execute(Jedis connection) {
                        return connection.sunion(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Set<String>>(worker, getGroupName(keys[0]), "sunion").execute();
    }

    @Override
    public Long sunionstore(final String dstkey, final String... keys) {
        final String[] wholeKeys = KeyMergeUtil.merge(dstkey, keys);
        if (wholeKeys == null || wholeKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.sunionstore(dstkey, keys);
                    }
                }.run(wholeKeys.length, wholeKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(wholeKeys[0]), "sunionstore").execute();
    }

    @Override
    public Long zinterstore(final String dstkey, final String... sets) {
        final String[] wholeKeys = KeyMergeUtil.merge(dstkey, sets);
        if (wholeKeys == null || wholeKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zinterstore(dstkey, sets);
                    }
                }.run(wholeKeys.length, wholeKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(wholeKeys[0]), "zinterstore").execute();
    }

    @Override
    public Long zinterstore(final String dstkey, final ZParams params, final String... sets) {
        final String[] mergedKeys = KeyMergeUtil.merge(dstkey, sets);
        if (mergedKeys == null || mergedKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zinterstore(dstkey, params, sets);
                    }
                }.run(mergedKeys.length, mergedKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(mergedKeys[0]), "zinterstore").execute();
    }

    @Override
    public Long zunionstore(final String dstkey, final String... sets) {
        final String[] mergedKeys = KeyMergeUtil.merge(dstkey, sets);
        if (mergedKeys == null || mergedKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zunionstore(dstkey, sets);
                    }
                }.run(mergedKeys.length, mergedKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(mergedKeys[0]), "zunionstore").execute();
    }

    @Override
    public Long zunionstore(final String dstkey, final ZParams params, final String... sets) {
        final String[] mergedKeys = KeyMergeUtil.merge(dstkey, sets);
        if (mergedKeys == null || mergedKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.zunionstore(dstkey, params, sets);
                    }
                }.run(mergedKeys.length, mergedKeys);

            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(mergedKeys[0]), "zunionstore").execute();
    }

    @Override
    public String brpoplpush(final String source, final String destination, final int timeout) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.brpoplpush(source, destination, timeout);
                    }
                }.run(2, source, destination);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(source), "brpoplpush").execute();
    }

    @Override
    public Long publish(final String channel, final String message) {
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.publish(channel, message);
                    }
                }.runWithAnyNode();
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(channel), "publish").execute();
    }

    @Override
    public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
        new TechwolfJedisClusterCommand<Integer>(connectionHandler, maxAttempts) {
            @Override
            public Integer execute(Jedis connection) {
                connection.subscribe(jedisPubSub, channels);
                return 0;
            }
        }.runWithAnyNode();
    }

    @Override
    public void psubscribe(final JedisPubSub jedisPubSub, final String... patterns) {
        new TechwolfJedisClusterCommand<Integer>(connectionHandler, maxAttempts) {
            @Override
            public Integer execute(Jedis connection) {
                connection.psubscribe(jedisPubSub, patterns);
                return 0;
            }
        }.runWithAnyNode();
    }

    @Override
    public Long bitop(final BitOP op, final String destKey, final String... srcKeys) {
        final String[] mergedKeys = KeyMergeUtil.merge(destKey, srcKeys);
        if (mergedKeys == null || mergedKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.bitop(op, destKey, srcKeys);
                    }
                }.run(mergedKeys.length, mergedKeys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(mergedKeys[0]), "bitop").execute();
    }

    @Override
    public String pfmerge(final String destkey, final String... sourcekeys) {
        final String[] mergedKeys = KeyMergeUtil.merge(destkey, sourcekeys);
        if (mergedKeys == null || mergedKeys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.pfmerge(destkey, sourcekeys);
                    }
                }.run(mergedKeys.length, mergedKeys);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(mergedKeys[0]), "pfmerge").execute();
    }

    @Override
    public long pfcount(final String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Long> worker = new TechwolfHystrixRedisWorker<Long>() {
            @Override
            public Long work() {
                return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
                    @Override
                    public Long execute(Jedis connection) {
                        return connection.pfcount(keys);
                    }
                }.run(keys.length, keys);
            }
        };
        return new RedisHystrixCommand<Long>(worker, getGroupName(keys[0]), "pfcount").execute();
    }

    @Override
    public Object eval(final String script, final int keyCount, final String... params) {
        if (params == null || params.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Object> worker = new TechwolfHystrixRedisWorker<Object>() {
            @Override
            public Object work() {
                return new TechwolfJedisClusterCommand<Object>(connectionHandler, maxAttempts) {
                    @Override
                    public Object execute(Jedis connection) {
                        return connection.eval(script, keyCount, params);
                    }
                }.run(keyCount, params);
            }
        };
        return new RedisHystrixCommand<Object>(worker, getGroupName(params[0]), "eval").execute();
    }

    @Override
    public Object eval(final String script, final String key) {
        TechwolfHystrixRedisWorker<Object> worker = new TechwolfHystrixRedisWorker<Object>() {
            @Override
            public Object work() {
                return new TechwolfJedisClusterCommand<Object>(connectionHandler, maxAttempts) {
                    @Override
                    public Object execute(Jedis connection) {
                        return connection.eval(script);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Object>(worker, getGroupName(key), "eval").execute();
    }

    @Override
    public Object eval(final String script, final List<String> keys, final List<String> args) {
        if (CollectionUtils.isEmpty(keys)) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Object> worker = new TechwolfHystrixRedisWorker<Object>() {
            @Override
            public Object work() {
                return new TechwolfJedisClusterCommand<Object>(connectionHandler, maxAttempts) {
                    @Override
                    public Object execute(Jedis connection) {
                        return connection.eval(script, keys, args);
                    }
                }.run(keys.size(), keys.toArray(new String[keys.size()]));
            }
        };
        return new RedisHystrixCommand<Object>(worker, getGroupName(keys.get(0)), "eval").execute();
    }

    @Override
    public Object evalsha(final String sha1, final int keyCount, final String... params) {
        if (params == null || params.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Object> worker = new TechwolfHystrixRedisWorker<Object>() {
            @Override
            public Object work() {
                return new TechwolfJedisClusterCommand<Object>(connectionHandler, maxAttempts) {
                    @Override
                    public Object execute(Jedis connection) {
                        return connection.evalsha(sha1, keyCount, params);
                    }
                }.run(keyCount, params);
            }
        };
        return new RedisHystrixCommand<Object>(worker, getGroupName(params[0]), "eval").execute();
    }

    @Override
    public Object evalsha(final String sha1, final List<String> keys, final List<String> args) {
        if (CollectionUtils.isEmpty(keys)) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }
        TechwolfHystrixRedisWorker<Object> worker = new TechwolfHystrixRedisWorker<Object>() {
            @Override
            public Object work() {
                return new TechwolfJedisClusterCommand<Object>(connectionHandler, maxAttempts) {
                    @Override
                    public Object execute(Jedis connection) {
                        return connection.evalsha(sha1, keys, args);
                    }
                }.run(keys.size(), keys.toArray(new String[keys.size()]));
            }
        };
        return new RedisHystrixCommand<Object>(worker, getGroupName(keys.get(0)), "evalsha").execute();
    }

    @Override
    public Object evalsha(final String script, final String key) {
        TechwolfHystrixRedisWorker<Object> worker = new TechwolfHystrixRedisWorker<Object>() {
            @Override
            public Object work() {
                return new TechwolfJedisClusterCommand<Object>(connectionHandler, maxAttempts) {
                    @Override
                    public Object execute(Jedis connection) {
                        return connection.evalsha(script);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Object>(worker, getGroupName(key), "evalsha").execute();
    }

    @Override
    public Boolean scriptExists(final String sha1, final String key) {
        TechwolfHystrixRedisWorker<Boolean> worker = new TechwolfHystrixRedisWorker<Boolean>() {
            @Override
            public Boolean work() {
                return new TechwolfJedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
                    @Override
                    public Boolean execute(Jedis connection) {
                        return connection.scriptExists(sha1);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<Boolean>(worker, getGroupName(key), "scriptExists").execute();
    }

    @Override
    public List<Boolean> scriptExists(final String key, final String... sha1) {
        TechwolfHystrixRedisWorker<List<Boolean>> worker = new TechwolfHystrixRedisWorker<List<Boolean>>() {
            @Override
            public List<Boolean> work() {
                return new TechwolfJedisClusterCommand<List<Boolean>>(connectionHandler, maxAttempts) {
                    @Override
                    public List<Boolean> execute(Jedis connection) {
                        return connection.scriptExists(sha1);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<List<Boolean>>(worker, getGroupName(key), "scriptExists").execute();
    }

    @Override
    public String scriptLoad(final String script, final String key) {
        TechwolfHystrixRedisWorker<String> worker = new TechwolfHystrixRedisWorker<String>() {
            @Override
            public String work() {
                return new TechwolfJedisClusterCommand<String>(connectionHandler, maxAttempts) {
                    @Override
                    public String execute(Jedis connection) {
                        return connection.scriptLoad(script);
                    }
                }.run(key);
            }
        };
        return new RedisHystrixCommand<String>(worker, getGroupName(key), "scriptLoad").execute();
    }

  /*
   * below methods will be removed at 3.0
   */

    /**
     * @see <a href="https://github.com/xetorthio/jedis/pull/878">issue#878</a>
     * @deprecated SetParams is scheduled to be introduced at next major release Please use setnx
     * instead for now
     */
    @Deprecated
    @Override
    public String set(String key, String value, String nxxx) {
        return setnx(key, value) == 1 ? "OK" : null;
    }

    /**
     * @deprecated unusable command, this will be removed at next major release.
     */
    @Deprecated
    @Override
    public List<String> blpop(final String arg) {
        return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
            @Override
            public List<String> execute(Jedis connection) {
                return connection.blpop(arg);
            }
        }.run(arg);
    }

    /**
     * @deprecated unusable command, this will be removed at next major release.
     */
    @Deprecated
    @Override
    public List<String> brpop(final String arg) {
        return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
            @Override
            public List<String> execute(Jedis connection) {
                return connection.brpop(arg);
            }
        }.run(arg);
    }

    /**
     * @deprecated Redis Cluster uses only db index 0, so it doesn't make sense. scheduled to be
     * removed on next major release
     */
    @Deprecated
    @Override
    public Long move(final String key, final int dbIndex) {
        return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.move(key, dbIndex);
            }
        }.run(key);
    }

    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long) And will be removed
     * on next major release
     *
     * @see <a href="https://github.com/xetorthio/jedis/issues/531">issue#531</a>
     */
    @Deprecated
    @Override
    public ScanResult<Entry<String, String>> hscan(final String key, final int cursor) {
        return new TechwolfJedisClusterCommand<ScanResult<Entry<String, String>>>(connectionHandler,
                maxAttempts) {
            @Override
            public ScanResult<Entry<String, String>> execute(Jedis connection) {
                return connection.hscan(key, cursor);
            }
        }.run(key);
    }

    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long) And will be removed
     * on next major release
     *
     * @see <a href="https://github.com/xetorthio/jedis/issues/531">issue#531</a>
     */
    @Deprecated
    @Override
    public ScanResult<String> sscan(final String key, final int cursor) {
        return new TechwolfJedisClusterCommand<ScanResult<String>>(connectionHandler, maxAttempts) {
            @Override
            public ScanResult<String> execute(Jedis connection) {
                return connection.sscan(key, cursor);
            }
        }.run(key);
    }

    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long) And will be removed
     * on next major release
     *
     * @see <a href="https://github.com/xetorthio/jedis/issues/531">issue#531</a>
     */
    @Deprecated
    @Override
    public ScanResult<Tuple> zscan(final String key, final int cursor) {
        return new TechwolfJedisClusterCommand<ScanResult<Tuple>>(connectionHandler, maxAttempts) {
            @Override
            public ScanResult<Tuple> execute(Jedis connection) {
                return connection.zscan(key, cursor);
            }
        }.run(key);
    }

    @Override
    public Long geoadd(final String key, final double longitude, final double latitude,
                       final String member) {
        return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.geoadd(key, longitude, latitude, member);
            }
        }.run(key);
    }

    @Override
    public Long geoadd(final String key, final Map<String, GeoCoordinate> memberCoordinateMap) {
        return new TechwolfJedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.geoadd(key, memberCoordinateMap);
            }
        }.run(key);
    }

    @Override
    public Double geodist(final String key, final String member1, final String member2) {
        return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
            @Override
            public Double execute(Jedis connection) {
                return connection.geodist(key, member1, member2);
            }
        }.run(key);
    }

    @Override
    public Double geodist(final String key, final String member1, final String member2,
                          final GeoUnit unit) {
        return new TechwolfJedisClusterCommand<Double>(connectionHandler, maxAttempts) {
            @Override
            public Double execute(Jedis connection) {
                return connection.geodist(key, member1, member2, unit);
            }
        }.run(key);
    }

    @Override
    public List<String> geohash(final String key, final String... members) {
        return new TechwolfJedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
            @Override
            public List<String> execute(Jedis connection) {
                return connection.geohash(key, members);
            }
        }.run(key);
    }

    @Override
    public List<GeoCoordinate> geopos(final String key, final String... members) {
        return new TechwolfJedisClusterCommand<List<GeoCoordinate>>(connectionHandler, maxAttempts) {
            @Override
            public List<GeoCoordinate> execute(Jedis connection) {
                return connection.geopos(key, members);
            }
        }.run(key);
    }

    @Override
    public List<GeoRadiusResponse> georadius(final String key, final double longitude,
                                             final double latitude, final double radius, final GeoUnit unit) {
        return new TechwolfJedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxAttempts) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadius(key, longitude, latitude, radius, unit);
            }
        }.run(key);
    }

    @Override
    public List<GeoRadiusResponse> georadius(final String key, final double longitude,
                                             final double latitude, final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return new TechwolfJedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxAttempts) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadius(key, longitude, latitude, radius, unit, param);
            }
        }.run(key);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(final String key, final String member,
                                                     final double radius, final GeoUnit unit) {
        return new TechwolfJedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxAttempts) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadiusByMember(key, member, radius, unit);
            }
        }.run(key);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(final String key, final String member,
                                                     final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return new TechwolfJedisClusterCommand<List<GeoRadiusResponse>>(connectionHandler, maxAttempts) {
            @Override
            public List<GeoRadiusResponse> execute(Jedis connection) {
                return connection.georadiusByMember(key, member, radius, unit, param);
            }
        }.run(key);
    }

    @Override
    public List<Long> bitfield(final String key, final String... arguments) {
        return new TechwolfJedisClusterCommand<List<Long>>(connectionHandler, maxAttempts) {
            @Override
            public List<Long> execute(Jedis connection) {
                return connection.bitfield(key, arguments);
            }
        }.run(key);
    }
}
