package redis.clients.techwolf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhaoyalong on 17-3-28.
 */
public class TechwolfJedisClusterInfoCache {

    private final Map<String, MasterSlaveNode> nodes = new HashMap<String, MasterSlaveNode>();
    private final Map<Integer, MasterSlaveNode> slots = new HashMap<Integer, MasterSlaveNode>();

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();
    private volatile boolean rediscovering;
    private final GenericObjectPoolConfig poolConfig;

    private int connectionTimeout;
    private int soTimeout;
    private String password;
    private String clientName;

    private static final int MASTER_NODE_INDEX = 2;

    public TechwolfJedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
                                         final int connectionTimeout, final int soTimeout, final String password, final String clientName) {
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.clientName = clientName;
    }

    private void discoverClusterMasterAndSlave(Jedis jedis) {
        String clusterNodesCommand = jedis.clusterNodes();
        String[] allNodes = clusterNodesCommand.split("\n");
        Map<String, Set<String>> tempMap = new HashMap<String, Set<String>>();
        Map<String, String> tempMap2 = new HashMap<String, String>();
        for (String allNode : allNodes) {
            String[] splits = allNode.split(" ");
            ClusterNodeObject clusterNodeObject =
                    new ClusterNodeObject(splits[0], splits[1],
                            splits[2].contains("master"), splits[3],
                            NumberUtils.toLong(splits[4], 0), NumberUtils.toLong(splits[5], 0), splits[6],
                            splits[7].equalsIgnoreCase("connected"), splits.length == 9 ? splits[8] : null);

            if (clusterNodeObject.isMaster()) {
                tempMap2.put(clusterNodeObject.getNodeId(), clusterNodeObject.getHostAndPort());
                MasterSlaveNode masterSlaveNode = nodes.get(clusterNodeObject.getHostAndPort());
                if (masterSlaveNode == null) {
                    masterSlaveNode = new MasterSlaveNode(clusterNodeObject);
                    nodes.put(masterSlaveNode.getMasterHostAndPort(), masterSlaveNode);
                    if (clusterNodeObject.getSlot() != null) {
                        int beginSlot = NumberUtils.toInt(clusterNodeObject.getSlot().split("-")[0]);
                        int endSlot = NumberUtils.toInt(clusterNodeObject.getSlot().split("-")[1]);
                        for (int i = beginSlot; i <= endSlot; i++) {
                            masterSlaveNode.getSlotList().add(i);
                        }
                    }
                }
                Set<String> slaveSet = tempMap.get(clusterNodeObject.getNodeId());
                if (slaveSet == null) {
                    tempMap.put(clusterNodeObject.getNodeId(), new HashSet<String>());
                } else if (!slaveSet.isEmpty()) {
                    masterSlaveNode.getSlaveHostAndPort().addAll(slaveSet);
                }
            } else {
                String masterHostAndPort = tempMap2.get(clusterNodeObject.getMasterId());
                if (StringUtils.isBlank(masterHostAndPort)) {
                    Set<String> set = tempMap.get(clusterNodeObject.getMasterId());
                    if (set == null) {
                        set = new HashSet<String>();
                        tempMap.put(clusterNodeObject.getMasterId(), set);
                    }
                    set.add(clusterNodeObject.getHostAndPort());
                } else {
                    nodes.get(masterHostAndPort).getSlaveHostAndPort().add(clusterNodeObject.getHostAndPort());
                }
            }
        }
    }

    public void discoverClusterNodesAndSlots(Jedis jedis) {
        w.lock();

        try {
            reset();
            discoverClusterMasterAndSlave(jedis);
            setupAllMasterAndSlaveNode();
        } finally {
            w.unlock();
        }
    }

    private void setupAllMasterAndSlaveNode() {
        for (Map.Entry<String, MasterSlaveNode> entry : nodes.entrySet()) {
            String hostAndPortStr = entry.getKey();
            String host = hostAndPortStr.split(":")[0];
            int port = NumberUtils.toInt(hostAndPortStr.split(":")[1]);
            HostAndPort hostAndPort = new HostAndPort(host, port);
            setupNodeIfNotExist(hostAndPort);
            assignSlotsToNode(entry.getValue().getSlotList(), hostAndPort);
        }
    }

    public void renewClusterSlots(Jedis jedis) {
        //If rediscovering is already in process - no need to start one more same rediscovering, just return
        if (!rediscovering) {
            try {
                w.lock();
                rediscovering = true;

                if (jedis != null) {
                    try {
                        discoverClusterMasterAndSlave(jedis);
                        setupAllMasterAndSlaveNode();
                        return;
                    } catch (JedisException e) {
                        //try nodes from all pools
                    }
                }

                for (JedisPool jp : getShuffledNodesPool()) {
                    try {
                        jedis = jp.getResource();
                        discoverClusterMasterAndSlave(jedis);
                        setupAllMasterAndSlaveNode();
                        return;
                    } catch (JedisConnectionException e) {
                        // try next nodes
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }
            } finally {
                rediscovering = false;
                w.unlock();
            }
        }
    }

    private HostAndPort generateHostAndPort(List<Object> hostInfos) {
        return new HostAndPort(SafeEncoder.encode((byte[]) hostInfos.get(0)),
                ((Long) hostInfos.get(1)).intValue());
    }

    public JedisPool setupNodeIfNotExist(HostAndPort node) {
        w.lock();
        try {
            String nodeKey = getNodeKey(node);
            MasterSlaveNode masterSlaveNode = nodes.get(nodeKey);
            JedisPool existingPool = masterSlaveNode.getMaster();
            if (existingPool != null) return existingPool;

            JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
                    connectionTimeout, soTimeout, password, 0, clientName, false, null, null, null);
            masterSlaveNode.setMaster(nodePool);

            if (masterSlaveNode.getSlaveHostAndPort() != null && masterSlaveNode.getSlave() == null) {
                masterSlaveNode.setSlave(new ArrayList<JedisPool>(masterSlaveNode.getSlaveHostAndPort().size()));
                for (String slave : masterSlaveNode.getSlaveHostAndPort()) {
                    String host = slave.split(":")[0];
                    int port = NumberUtils.toInt(slave.split(":")[1]);
                    JedisPool slavePool = new JedisPool(poolConfig, host, port,
                            connectionTimeout, soTimeout, password, 0, clientName, false, null, null, null, false);
                    masterSlaveNode.getSlave().add(slavePool);
                }
            }

            return nodePool;
        } finally {
            w.unlock();
        }
    }

    public JedisPool setupNodeIfNotExist(HostAndPort node, boolean ssl) {
        w.lock();
        try {
            String nodeKey = getNodeKey(node);
            MasterSlaveNode masterSlaveNode = nodes.get(nodeKey);
            JedisPool existingPool = masterSlaveNode.getMaster();
            if (existingPool != null) return existingPool;

            JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
                    connectionTimeout, soTimeout, password, 0, clientName, ssl, null, null, null);
            masterSlaveNode.setMaster(nodePool);

            if (masterSlaveNode.getSlaveHostAndPort() != null && masterSlaveNode.getSlave() == null) {
                masterSlaveNode.setSlave(new ArrayList<JedisPool>(masterSlaveNode.getSlaveHostAndPort().size()));
                for (String slave : masterSlaveNode.getSlaveHostAndPort()) {
                    String host = slave.split(":")[0];
                    int port = NumberUtils.toInt(slave.split(":")[1]);
                    JedisPool slavePool = new JedisPool(poolConfig, host, port,
                            connectionTimeout, soTimeout, password, 0, clientName, ssl, null, null, null);
                    masterSlaveNode.getSlave().add(slavePool);
                }
            }
            return nodePool;
        } finally {
            w.unlock();
        }
    }

    public JedisPool setupNodeIfNotExist(HostAndPort node, boolean ssl, SSLSocketFactory sslSocketFactory,
                                         SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        w.lock();
        try {
            String nodeKey = getNodeKey(node);
            MasterSlaveNode masterSlaveNode = nodes.get(nodeKey);
            JedisPool existingPool = masterSlaveNode.getMaster();
            if (existingPool != null) return existingPool;

            JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
                    connectionTimeout, soTimeout, password, 0, null, ssl, sslSocketFactory, sslParameters,
                    hostnameVerifier);
            masterSlaveNode.setMaster(nodePool);

            if (masterSlaveNode.getSlaveHostAndPort() != null && masterSlaveNode.getSlave() == null) {
                masterSlaveNode.setSlave(new ArrayList<JedisPool>(masterSlaveNode.getSlaveHostAndPort().size()));
                for (String slave : masterSlaveNode.getSlaveHostAndPort()) {
                    String host = slave.split(":")[0];
                    int port = NumberUtils.toInt(slave.split(":")[1]);
                    JedisPool slavePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
                            connectionTimeout, soTimeout, password, 0, null, ssl, sslSocketFactory, sslParameters,
                            hostnameVerifier);
                    masterSlaveNode.getSlave().add(slavePool);
                }
            }
            return nodePool;
        } finally {
            w.unlock();
        }
    }

    public void assignSlotToNode(int slot, HostAndPort targetNode) {
        w.lock();
        try {
            setupNodeIfNotExist(targetNode);
            MasterSlaveNode masterSlaveNode = nodes.get(targetNode.toString());
            slots.put(slot, masterSlaveNode);
        } finally {
            w.unlock();
        }
    }

    public void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode) {
        w.lock();
        try {
            setupNodeIfNotExist(targetNode);
            MasterSlaveNode masterSlaveNode = nodes.get(targetNode.toString());
            for (Integer slot : targetSlots) {
                slots.put(slot, masterSlaveNode);
            }
        } finally {
            w.unlock();
        }
    }

    public JedisPool getNode(String nodeKey) {
        r.lock();
        try {
            return nodes.get(nodeKey).getMaster();
        } finally {
            r.unlock();
        }
    }

    public JedisPool getSlotReadPool(int slot) {
        r.lock();
        try {
            MasterSlaveNode masterSlaveNode = slots.get(slot);
            return masterSlaveNode.getSlaveByStrategy(MasterSlaveNode.SlaveStrategy.ROUND_ROBIN);
        } finally {
            r.unlock();
        }
    }

    public JedisPool getSlotWritePool(int slot) {
        r.lock();
        try {
            return slots.get(slot).getMaster();
        } finally {
            r.unlock();
        }
    }

    public JedisPool getSlotPool(int slot) {
        r.lock();
        try {
            return slots.get(slot).getMaster();
        } finally {
            r.unlock();
        }
    }

    public Map<String, JedisPool> getNodes() {
        r.lock();
        try {
            Map<String, JedisPool> map = new HashMap<String, JedisPool>();
            for (Map.Entry<String, MasterSlaveNode> entry : nodes.entrySet()) {
                map.put(entry.getValue().getMasterHostAndPort(), entry.getValue().getMaster());
            }
            return new HashMap<String, JedisPool>(map);
        } finally {
            r.unlock();
        }
    }

    public List<JedisPool> getShuffledNodesPool() {
        r.lock();
        try {
            List<JedisPool> pools = new ArrayList<JedisPool>();
            for (MasterSlaveNode node : nodes.values()) {
                pools.add(node.getMaster());
                if (node.getSlave() != null) {
                    pools.addAll(node.getSlave());
                }
            }
            Collections.shuffle(pools);
            return pools;
        } finally {
            r.unlock();
        }
    }

    /**
     * Clear discovered nodes collections and gently release allocated resources
     */
    public void reset() {
        w.lock();
        try {
            for (MasterSlaveNode node : nodes.values()) {
                try {
                    if (node != null) {
                        node.destroy();
                    }
                } catch (Exception e) {
                    // pass
                }
            }
            nodes.clear();
            slots.clear();
        } finally {
            w.unlock();
        }
    }

    public static String getNodeKey(HostAndPort hnp) {
        return hnp.getHost() + ":" + hnp.getPort();
    }

    public static String getNodeKey(Client client) {
        return client.getHost() + ":" + client.getPort();
    }

    public static String getNodeKey(Jedis jedis) {
        return getNodeKey(jedis.getClient());
    }

    private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
        List<Integer> slotNums = new ArrayList<Integer>();
        for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
                .intValue(); slot++) {
            slotNums.add(slot);
        }
        return slotNums;
    }

    public static void main(String[] args) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        TechwolfJedisClusterInfoCache cache = new TechwolfJedisClusterInfoCache(config, 2000, 2000, null, null);
        Jedis jedis = new Jedis("192.168.1.167", 7000);
        cache.discoverClusterNodesAndSlots(jedis);
        System.out.println();
    }


}
