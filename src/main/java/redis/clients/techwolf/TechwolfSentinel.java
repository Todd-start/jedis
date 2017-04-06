package redis.clients.techwolf;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by zhaoyalong on 17-4-5.
 */
public class TechwolfSentinel {

    private JedisPool jedisPool;

    private HostAndPort hostAndPort;

    private ChannelListener channelListener;

    private Logger log = Logger.getLogger(getClass().getName());

    public TechwolfSentinel(String sentinel, JedisPoolConfig config) {
        hostAndPort = HostAndPort.parseString(sentinel);
        jedisPool = new JedisPool(config, hostAndPort.getHost(), hostAndPort.getPort());
    }

    public void close() {
        if (channelListener != null) {
            channelListener.shutdown();
        }
        jedisPool.destroy();
    }

    public void addChannelListener(JedisPubSub jedisPubSub, String... arr) {
        channelListener = new ChannelListener(jedisPubSub, arr);
        channelListener.start();
    }

    class ChannelListener extends Thread {

        private AtomicBoolean running = new AtomicBoolean(false);
        private Jedis jedis = jedisPool.getResource();
        private String[] channels;
        private long subscribeRetryWaitTimeMillis = 5000;
        private JedisPubSub jedisPubSub;

        public ChannelListener(JedisPubSub jedisPubSub, String... arr) {
            super(String.format("ChannelListener-[%s:%d]", hostAndPort.getHost(), hostAndPort.getPort()));
            this.channels = arr;
            this.jedisPubSub = jedisPubSub;
        }

        public void shutdown() {
            try {
                log.fine("Shutting down listener on " + hostAndPort.getHost() + ":" + hostAndPort.getPort());
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                if (jedis != null) {
                    jedis.disconnect();
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "Caught exception while shutting down: ", e);
            }
        }

        @Override
        public void run() {
            running.set(true);
            while (running.get()) {
                try {
                    if (!running.get()) {
                        break;
                    }
                    jedis.subscribe(jedisPubSub, channels);
                } catch (JedisException e) {
                    if (running.get()) {
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            log.log(Level.SEVERE, "Sleep interrupted: ", e1);
                        }
                    } else {
                        log.fine("Unsubscribing from Sentinel at " + hostAndPort.getHost() + ":" + hostAndPort.getPort());
                    }
                } finally {
                    jedis.close();
                }
            }
        }
    }

}
