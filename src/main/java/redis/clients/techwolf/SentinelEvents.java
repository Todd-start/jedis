package redis.clients.techwolf;

/**
 * Created by zhaoyalong on 17-4-6.
 * sentinel事件
 * <instance-type> <name> <ip> <port> @ <master-name> <master-ip> <master-port>
 */
public final class SentinelEvents {
    /**
     * 主观下线
     * slave 192.168.1.167:7000 192.168.1.167 7000 @ test1 192.168.1.167 7001
     *
     * master test1 192.168.1.167 7001
     */
    public static final String SDOWN_PLUS = "+sdown";
    /**
     * 主观下线结束
     */
    public static final String SDOWN_MINUS = "-sdown";

    /**
     * 客观下线
     *
     * master test1 192.168.1.167 7001 #quorum 1/1
     */
    public static final String ODOWN_PLUS = "+odown";
    /**
     * 客观下线结束
     */
    public static final String ODOWN_MINUS = "-odown";
    /**
     * 主库变换
     * <master name> <oldip> <oldport> <newip> <newport>
     */
    public static final String SWITCH_MASTER = "switch-master";
    /**
     * 故障转移结束
     */
    public static final String FAILOVER_END = "failover-end";
    /**
     * 新丛库增加
     * slave 192.168.1.167:7001 192.168.1.167 7001 @ test1 192.168.1.167 7000
     */
    public static final String SLAVE_PLUS = "+slave";

}
