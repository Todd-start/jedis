package redis.clients.jedis;

import com.netflix.hystrix.*;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoyalong on 17-8-9.
 */
public class hystrix {

    @Test
    public void test() throws InterruptedException {
        ExecutorService service = new ThreadPoolExecutor(100,
                100, 5, TimeUnit.HOURS, new ArrayBlockingQueue<Runnable>(100000000));
        for (int i = 0; i < 10000000; i++) {
            final int index = i;
//            Thread.sleep(100);
            service.submit(new Runnable() {
                @Override
                public void run() {
                    new Task(index).execute();
                }
            });
//            service.submit(new Runnable() {
//                @Override
//                public void run() {
//                    new Task1(index).execute();
//                }
//            });
//            service.submit(new TaskB(index));
        }
        System.out.println("fin");
        Thread.sleep(2000000);
    }

}

class Task extends HystrixCommand {
    private int index;

    public Task(int index) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(String.valueOf(index % 3)))
                .andCommandKey(HystrixCommandKey.Factory.asKey(String.format("%s", String.valueOf(index % 3))))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(String.valueOf(index%3)))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withCircuitBreakerEnabled(true)
//                        .withCircuitBreakerForceOpen(true)
                        .withCircuitBreakerRequestVolumeThreshold(100)//10秒钟内的请求次数达到这个才计算
                        .withCircuitBreakerSleepWindowInMilliseconds(30000)//熔断器中断请求30秒后会进入半打开状态,放部分流量过去重试
                        .withCircuitBreakerErrorThresholdPercentage(100)//错误率达到80开启熔断保护
                        .withExecutionTimeoutEnabled(true)
                        .withExecutionTimeoutInMilliseconds(100))//使用dubbo的超时，禁用这里的超时
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(20)));
        this.index = index;
    }

    @Override
    protected Object run() throws Exception {
        if (index % 3 == 0) {
//            Thread.sleep(5000);
            throw new RuntimeException();
        }
        System.out.println("running");
        return null;
    }

    @Override
    protected Object getFallback() {
        System.out.println("fallback" + index%3 );
        return null;
    }
}

