package redis.clients.jedis.exceptions;

public class JedisException extends RuntimeException {
    private static final long serialVersionUID = -2946266495682282677L;
    //获取报错地址进行细粒度操作
    private String host;
    private int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public JedisException(String message, String host, int port) {
        super(message);
        this.host = host;
        this.port = port;
    }

    public JedisException(String message) {
        super(message);
    }

    public JedisException(Throwable e) {
        super(e);
    }

    public JedisException(Throwable e,String host,int port) {
        super(e);
        this.host = host;
        this.port = port;
    }

    public JedisException(String message, Throwable cause) {
        super(message, cause);
    }

    public JedisException(String message, Throwable cause,String host,int port) {
        super(message, cause);
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return "JedisException{" +
                "host='" + host + '\'' +
                ", port=" + port +
                "} " + super.toString();
    }
}
