package redis.clients.jedis.tests;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.techwolf.TechwolfJedisClusterInfoCache;

/**
 * Created by zhaoyalong on 17-4-7.
 */
public class TechwolfTest {

    TechwolfJedisClusterInfoCache cache = null;

    @Before
    public void before() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        cache = new TechwolfJedisClusterInfoCache(config, 2000, 2000, null, null, true);
        Jedis jedis = new Jedis("192.168.1.167", 7000);
        cache.discoverClusterNodesAndSlots(jedis);
    }

    @After
    public void after() {
        cache.reset();
    }

    @Test
    public void masterAndSlaveTest() {
        Assert.assertEquals("", 3, cache.getNodes().size());
    }

    @Test
    public void testRenew() {
        JedisPool jedisPool = cache.getSlotReadPool(1);
        Assert.assertNotNull(jedisPool);
        Jedis jedis = jedisPool.getResource();
        Assert.assertNotNull(jedis);
        cache.renewClusterSlots(jedis);
        Assert.assertEquals("", 3, cache.getNodes().size());
    }

    @Test
    public void testSlavePlus() {
        //TODO
    }
}
