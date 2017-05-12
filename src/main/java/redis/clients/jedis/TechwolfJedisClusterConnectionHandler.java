package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.techwolf.QueryContext;
import redis.clients.techwolf.TechwolfJedisClusterInfoCache;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public abstract class TechwolfJedisClusterConnectionHandler implements Closeable {

    protected static final ThreadLocal<QueryContext> queryContextThreadLocal = new ThreadLocal<QueryContext>();

    protected final TechwolfJedisClusterInfoCache cache;

    public TechwolfJedisClusterConnectionHandler(Set<HostAndPort> nodes,
                                                 final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String clientName) {
        this(nodes,poolConfig,connectionTimeout,soTimeout,password,clientName,false);
    }

    public TechwolfJedisClusterConnectionHandler(Set<HostAndPort> nodes,
                                                 final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String clientName,boolean useSlave) {
        this.cache = new TechwolfJedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password, clientName, useSlave);
        initializeSlotsCache(nodes, poolConfig, password, clientName);
    }

    abstract Jedis getConnection();

    abstract Jedis getConnectionFromSlot(int slot);

    public Jedis getConnectionFromNode(HostAndPort node) {
        return cache.setupNodeIfNotExist(node).getResource();
    }

    public Map<String, JedisPool> getNodes() {
        return cache.getNodes();
    }

    private void initializeSlotsCache(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig, String password, String clientName) {
        for (HostAndPort hostAndPort : startNodes) {
            Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            try {
                if (password != null) {
                    jedis.auth(password);
                }
                if (clientName != null) {
                    jedis.clientSetname(clientName);
                }
                cache.discoverClusterNodesAndSlots(jedis);
                break;
            } catch (JedisConnectionException e) {
                // try next nodes
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    public void renewSlotCache() {
        cache.renewClusterSlots(null);
    }

    public void renewSlotCache(String host,int port) {
        cache.renewClusterSlots(host,port);
    }

    public void renewSlotCache(Jedis jedis) {
        cache.renewClusterSlots(jedis);
    }

    @Override
    public void close() {
        cache.reset();
    }


    public TechwolfJedisClusterConnectionHandler read() {
        queryContextThreadLocal.set(new QueryContext(QueryContext.OP_READ));
        return this;
    }

    public TechwolfJedisClusterConnectionHandler write() {
        queryContextThreadLocal.set(new QueryContext(QueryContext.OP_WRITE));
        return this;
    }

    public void removeQueryContext() {
        queryContextThreadLocal.remove();
    }

    public void renewSlotCache(Jedis connection, int slot, HostAndPort targetNode) {
        cache.renewSlotCache(connection, slot, targetNode);
    }

    public boolean badJedis(Jedis jedis){
        return cache.badJedis(jedis.getHost()+":" + jedis.getPort());
    }
}
