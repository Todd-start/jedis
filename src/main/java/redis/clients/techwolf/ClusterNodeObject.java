package redis.clients.techwolf;

import org.apache.commons.lang3.math.NumberUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaoyalong on 17-3-28.
 * <id> <ip:port> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
 */
public class ClusterNodeObject {

    private String nodeId;

    private String hostAndPort;

    private boolean master;

    private String masterId;

    private long pingSend;

    private long pongRecv;

    private String configEpoch;

    private boolean linkState;

    private String slot;

    public ClusterNodeObject(String id, String hostAndPort,
                             boolean master, String masterId,
                             long pingSend, long pongRecv,String configEpoch,
                             boolean linkState,String slot) {
        this.nodeId = id;
        this.hostAndPort = hostAndPort;
        this.master = master;
        this.masterId = masterId;
        this.pingSend = pingSend;
        this.pongRecv = pongRecv;
        this.configEpoch = configEpoch;
        this.linkState = linkState;
        this.slot = slot;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    public void setHostAndPort(String hostAndPort) {
        this.hostAndPort = hostAndPort;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getMasterId() {
        return masterId;
    }

    public void setMasterId(String masterId) {
        this.masterId = masterId;
    }

    public long getPingSend() {
        return pingSend;
    }

    public void setPingSend(long pingSend) {
        this.pingSend = pingSend;
    }

    public long getPongRecv() {
        return pongRecv;
    }

    public void setPongRecv(long pongRecv) {
        this.pongRecv = pongRecv;
    }

    public String getConfigEpoch() {
        return configEpoch;
    }

    public void setConfigEpoch(String configEpoch) {
        this.configEpoch = configEpoch;
    }

    public boolean isLinkState() {
        return linkState;
    }

    public void setLinkState(boolean linkState) {
        this.linkState = linkState;
    }

    public String getSlot() {
        return slot;
    }

    public void setSlot(String slot) {
        this.slot = slot;
    }

    private Map<HostAndPort, ClusterNodeObject> getClusterNodes(Jedis jedis) {
        Map<HostAndPort, ClusterNodeObject> nodeMap = new HashMap<HostAndPort, ClusterNodeObject>();
        String clusterNodesCommand = jedis.clusterNodes();
        String[] allNodes = clusterNodesCommand.split("\n");
        for (String allNode : allNodes) {
            String[] splits = allNode.split(" ");
            ClusterNodeObject clusterNodeObject =
                    new ClusterNodeObject(splits[0], splits[1],
                            splits[2].contains("master"), splits[3],
                            NumberUtils.toLong(splits[4], 0), NumberUtils.toLong(splits[5], 0), splits[6],
                            splits[7].equalsIgnoreCase("connected"), splits.length == 9 ? splits[8] : null);
            String[] arr = clusterNodeObject.getHostAndPort().split(":");
            HostAndPort hostAndPort = new HostAndPort(arr[0], NumberUtils.toInt(arr[1]));
            nodeMap.put(hostAndPort, clusterNodeObject);
        }
        return nodeMap;
    }
}
