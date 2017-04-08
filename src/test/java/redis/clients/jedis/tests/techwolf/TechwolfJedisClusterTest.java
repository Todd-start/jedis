package redis.clients.jedis.tests.techwolf;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.TechwolfJedisCluster;
import redis.clients.techwolf.TechwolfJedisConfig;

import java.util.Map;

/**
 * Created by zhaoyalong on 17-4-8.
 */
public class TechwolfJedisClusterTest {

    TechwolfJedisCluster techwolfJedisCluster = null;

    @Before
    public void before() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        HostAndPort hostAndPort = new HostAndPort("192.168.1.167", 8000);
        TechwolfJedisConfig techwolfJedisConfig = new TechwolfJedisConfig();
        techwolfJedisConfig.setHostAndPort(hostAndPort);
        techwolfJedisConfig.setPoolConfig(config);
        techwolfJedisCluster = new TechwolfJedisCluster(techwolfJedisConfig);
    }

    @After
    public void after() {
        Map<String, JedisPool> map = techwolfJedisCluster.getClusterNodes();
        for (JedisPool jedisPool : map.values()) {
            try {
                jedisPool.getResource().flushAll();
            } catch (Exception e) {

            } finally {
                jedisPool.close();
            }
        }
    }

    @Test
    public void get() {
        set();
        Assert.assertEquals("set", "11", techwolfJedisCluster.get("test1"));
    }

    @Test
    public void set() {
        Assert.assertEquals("set", "OK", techwolfJedisCluster.set("test1", "11"));
    }
}
