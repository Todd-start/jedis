package redis.clients.jedis.exceptions;

public class JedisConnectionException extends JedisException {
  private static final long serialVersionUID = 3878126572474819403L;

  public JedisConnectionException(String message) {
    super(message);
  }

  public JedisConnectionException(String message,String host,int port) {
    super(message,host,port);
  }

  public JedisConnectionException(Throwable cause) {
    super(cause);
  }

  public JedisConnectionException(Throwable cause,String host,int port) {
    super(cause,host,port);
  }

  public JedisConnectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public JedisConnectionException(String message, Throwable cause,String host,int port) {
    super(message, cause,host,port);
  }
}
