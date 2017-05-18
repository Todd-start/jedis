package redis.clients.techwolf.exceptions;

/**
 * Created by zhaoyalong on 17-5-17.
 */
public class TechwolfNodeBrokenException extends RuntimeException {

    private String host;

    private int port;

    public TechwolfNodeBrokenException(String host, int port) {
        super("node broken " + host + ":" + port);
        this.host = host;
        this.port = port;
    }
}
