package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.techwolf.QueryContext;

import java.util.List;
import java.util.Set;

public class TechwolfJedisSlotBasedConnectionHandler extends TechwolfJedisClusterConnectionHandler {

    public TechwolfJedisSlotBasedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String clientName) {
        this(nodes, poolConfig, connectionTimeout, soTimeout, password, clientName,false);
    }

    public TechwolfJedisSlotBasedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String clientName,boolean useSlave) {
        super(nodes, poolConfig, connectionTimeout, soTimeout, password, clientName,useSlave);
    }


    @Override
    public Jedis getConnection() {
        List<JedisPool> pools = cache.getShuffledNodesPool();
        for (JedisPool pool : pools) {
            Jedis jedis = null;
            try {
                jedis = pool.getResource();
                if (jedis == null) {
                    continue;
                }
                String result = jedis.ping();
                if (result.equalsIgnoreCase("pong")) return jedis;
                jedis.close();
            } catch (JedisException ex) {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        throw new JedisNoReachableClusterNodeException("No reachable node in cluster");
    }

    @Override
    public Jedis getConnectionFromSlot(int slot) {
        QueryContext queryContext = queryContextThreadLocal.get();
        JedisPool connectionPool = null;
        if (queryContext != null && queryContext.isRead()) {
            connectionPool = cache.getSlotReadPool(slot);
        }
        if (connectionPool == null) {
            connectionPool = cache.getSlotWritePool(slot);
        }
        if (connectionPool != null) {
            // It can't guaranteed to get valid connection because of node
            // assignment
            return connectionPool.getResource();
        } else {
            renewSlotCache(); //It's abnormal situation for cluster mode, that we have just nothing for slot, try to rediscover state
            connectionPool = cache.getSlotWritePool(slot);
            if (connectionPool != null) {
                return connectionPool.getResource();
            } else {
                //no choice, fallback to new connection to random node
                return getConnection();
            }
        }
    }

}
