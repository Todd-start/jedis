package redis.clients.techwolf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhaoyalong on 17-4-8.
 */
public class TechwolfJedisConfig {

    /**
     * 是否启动丛库可用。
     * 如果启动则在发现集群有丛库时,丛库会处理读请求。并且支持在线添加从库
     * 否则会忽略一切丛库,不会建立从的连接,也不支持动态添加丛库处理请求
     */
    private boolean useSlave = false;

    /**
     * jedis连接池配置
     */
    private GenericObjectPoolConfig poolConfig;

    /**
     * jedis建立连接的超时时间
     */
    private int connectionTimeout = 2000;

    /**
     * jedis socket timeout
     */
    private int soTimeout = 2000;

    /**
     * redis密码
     */
    private String password;

    /**
     * 客户端名称
     */
    private String clientName;

    /**
     * 最大尝试次数和分片相关
     */
    private int maxAttempts = 5;

    /**
     * 启动连接点,靠此点发现整个集群，形如： ip:port;ip:port
     */
    private String hostAndPortStr;

    public boolean isUseSlave() {
        return useSlave;
    }

    public void setUseSlave(boolean useSlave) {
        this.useSlave = useSlave;
    }

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }


    public String getHostAndPortStr() {
        return hostAndPortStr;
    }

    public void setHostAndPortStr(String hostAndPortStr) {
        this.hostAndPortStr = hostAndPortStr;
    }


    public Set<HostAndPort> getHostAndPortSet() {
        if (StringUtils.isBlank(hostAndPortStr)) {
            throw new IllegalArgumentException("TechwolfJedisConfig.hostAndPortStr is blank!!!");
        }

        Set<HostAndPort> hostAndPortSet = new HashSet<>();
        String[] arr = hostAndPortStr.split(";");
        for (int i = 0; i < arr.length; ++i) {
            String str = arr[i];
            String[] hostPort = str.split(":");
            if (hostPort.length != 2) {
                continue;
            }

            hostAndPortSet.add(new HostAndPort(hostPort[0], NumberUtils.toInt(hostPort[1])));
        }

        if (hostAndPortSet.isEmpty()) {
            throw new IllegalStateException("TechwolfJedisConfig.hostAndPortSet is empty!!!");
        }
        return hostAndPortSet;
    }
}
