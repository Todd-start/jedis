package redis.clients.techwolf;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhaoyalong on 17-4-1.
 * redis保护神　每个cache实例一个此对象用来链接
 * Sentinel 监听node变化,主动进行node节点cache修复
 */
public class TechwolfNodeSaint {

    private static final int DEFAULT_MAX_TOTAL = 3;

    private static final int DEFAULT_IDLE = 1;

    private static final int DEFAULT_MAX_WAIT = 2000;

    private final Set<String> sentinels = new HashSet<String>();

    private Set<TechwolfSentinel> sentinelSet;

    private JedisPoolConfig poolConfig;

    private JedisPubSub jedisPubSubListener;

    /**
     * ip:port;ip:port
     */
    public TechwolfNodeSaint(JedisPubSub jedisPubSub) {
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(DEFAULT_MAX_TOTAL);
        poolConfig.setMaxWaitMillis(DEFAULT_MAX_WAIT);
        poolConfig.setMaxIdle(DEFAULT_IDLE);
        poolConfig.setMinIdle(DEFAULT_IDLE);
        jedisPubSubListener = jedisPubSub;
    }

    public void init() {
        sentinels.add("192.168.1.31:26379");
        sentinelSet = new HashSet<TechwolfSentinel>(sentinels.size());
        for (String sentinel : sentinels) {
            sentinelSet.add(new TechwolfSentinel(sentinel, poolConfig));
        }
        addChannelEvents(SentinelEvents.SWITCH_MASTER,
                SentinelEvents.SLAVE_PLUS, SentinelEvents.SDOWN_PLUS, SentinelEvents.ODOWN_PLUS,
                SentinelEvents.ODOWN_MINUS,SentinelEvents.SDOWN_MINUS);
    }

    private void addChannelEvents(String... arr) {
        for (TechwolfSentinel techwolfSentinel : sentinelSet) {
            techwolfSentinel.addChannelListener(jedisPubSubListener, arr);
        }
    }
}
