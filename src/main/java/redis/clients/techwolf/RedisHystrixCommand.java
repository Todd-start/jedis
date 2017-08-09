package redis.clients.techwolf;

import com.netflix.hystrix.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.TechwolfJedisClusterCommand;

/**
 * Created by zhaoyalong on 17-8-9.
 */
public class RedisHystrixCommand<T> extends HystrixCommand<T> {

    private static Log logger = LogFactory.getLog(RedisHystrixCommand.class);

    private TechwolfJedisClusterCommand<T> command;

    private int keyCount;

    private String[] keys;

    private String key;

    public RedisHystrixCommand(TechwolfJedisClusterCommand<T> command, String groupName, String commandKey) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(groupName))
                .andCommandKey(HystrixCommandKey.Factory.asKey(String.format("%s_%s", groupName, commandKey)))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withCircuitBreakerEnabled(true)
                        .withCircuitBreakerForceOpen(true)
                        .withCircuitBreakerRequestVolumeThreshold(5)//10秒钟内至少5此请求失败，熔断器才发挥起作用
                        .withCircuitBreakerSleepWindowInMilliseconds(30000)//熔断器中断请求30秒后会进入半打开状态,放部分流量过去重试
                        .withCircuitBreakerErrorThresholdPercentage(80)//错误率达到80开启熔断保护
                        .withExecutionTimeoutEnabled(false))//使用dubbo的超时，禁用这里的超时
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(100)));
        this.command = command;
    }

    @Override
    protected T getFallback() {
        logger.warn("fall back return ");
        return null;
    }

    @Override
    protected T run() throws Exception {
        if (keys != null) {
            return command.run(keyCount, keys);
        } else {
            return command.run(key);
        }
    }

    public T run(int keyCount, String... keys) {
        this.keyCount = keyCount;
        this.keys = keys;
        return execute();
    }

    public T run(String key) {
        this.key = key;
        return execute();
    }

}