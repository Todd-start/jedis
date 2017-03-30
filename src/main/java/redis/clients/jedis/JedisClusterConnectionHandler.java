package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.techwolf.QueryContext;
import redis.clients.techwolf.TechwolfJedisClusterInfoCache;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public abstract class JedisClusterConnectionHandler implements Closeable {

  protected static final ThreadLocal<QueryContext> queryContextThreadLocal = new ThreadLocal<QueryContext>();

  protected final TechwolfJedisClusterInfoCache cache;

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
                                       final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, password, null);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
          final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String clientName) {
    this.cache = new TechwolfJedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password, clientName,false);
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

  public void renewSlotCache(Jedis jedis) {
    cache.renewClusterSlots(jedis);
  }

  @Override
  public void close() {
    cache.reset();
  }


  public JedisClusterConnectionHandler read() {
    queryContextThreadLocal.set(new QueryContext(QueryContext.OP_READ));
    return this;
  }
  public JedisClusterConnectionHandler write() {
    queryContextThreadLocal.set(new QueryContext(QueryContext.OP_WRITE));
    return this;
  }

  public void removeQueryContext(){
    queryContextThreadLocal.remove();
  }

}
