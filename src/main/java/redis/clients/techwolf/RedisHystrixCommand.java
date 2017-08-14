package redis.clients.techwolf;

import com.netflix.hystrix.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by zhaoyalong on 17-8-9.
 */
public class RedisHystrixCommand<T> extends HystrixCommand<T>{

    private static Log logger = LogFactory.getLog(RedisHystrixCommand.class);

    private TechwolfHystrixRedisWorker<T> worker;

    public RedisHystrixCommand(TechwolfHystrixRedisWorker worker, String groupName, String commandKey) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(groupName))
                .andCommandKey(HystrixCommandKey.Factory.asKey(String.format("%s_%s", groupName, commandKey)))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withCircuitBreakerEnabled(true)
                        .withCircuitBreakerRequestVolumeThreshold(20)//10秒钟内至少5此请求，熔断器才发挥起作用
                        .withCircuitBreakerSleepWindowInMilliseconds(30000)//熔断器中断请求30秒后会进入半打开状态,放部分流量过去重试
                        .withCircuitBreakerErrorThresholdPercentage(50)//错误率达到80开启熔断保护
                        .withExecutionTimeoutEnabled(true)
                        .withExecutionTimeoutInMilliseconds(500))//使用dubbo的超时，禁用这里的超时
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(30)));
        this.worker = worker;
    }

    @Override
    protected T getFallback() {
        System.out.println("fall back return");
        logger.warn("fall back return ");
        return null;
    }

    @Override
    protected T run() throws Exception {
        return worker.work();
    }
}