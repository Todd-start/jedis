package redis.clients.techwolf;


/**
 * Created by zhaoyalong on 17-8-14.
 */
public interface TechwolfHystrixRedisWorker<T> {
    T work();
}
