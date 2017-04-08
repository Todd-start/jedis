package redis.clients.techwolf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Created by zhaoyalong on 17-3-28.
 */
public class TechwolfJedisClusterInfoCache {

    private Logger log = Logger.getLogger(getClass().getName());
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
    private boolean useSlave = false;
    private final TechwolfNodeSaint techwolfNodeSaint = new TechwolfNodeSaint(new CachePubSub());

    private static final int MASTER_NODE_INDEX = 2;

    public TechwolfJedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
                                         final int connectionTimeout, final int soTimeout,
                                         final String password,
                                         final String clientName, final boolean useSlave) {
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.clientName = clientName;
        this.useSlave = useSlave;
        techwolfNodeSaint.init();
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
            if (!clusterNodeObject.isLinkState()) {
                continue;
            }
            if (clusterNodeObject.isMaster()) {
                tempMap2.put(clusterNodeObject.getNodeId(), clusterNodeObject.getHostAndPort());
                MasterSlaveNode masterSlaveNode = nodes.get(clusterNodeObject.getHostAndPort());
                if (masterSlaveNode == null) {
                    //如果没有说明是新主　则new一个放到node里
                    masterSlaveNode = new MasterSlaveNode(clusterNodeObject);
                    nodes.put(masterSlaveNode.getMasterHostAndPort(), masterSlaveNode);
                } else {
                    //如果以存在的node配置版本没有变化则不进行销毁
                    //如果node里有，重新生成的时候要释放之前的slave
                    if (!clusterNodeObject.getConfigEpoch().equals(masterSlaveNode.getConfigEpoch())) {
                        masterSlaveNode.destroySlave();
                    }
                }
                //重新分配槽
                if (clusterNodeObject.getSlot() != null) {
                    //每个主只能进来一次所以重新分槽时先删除
                    masterSlaveNode.clearSlot();
                    int beginSlot = NumberUtils.toInt(clusterNodeObject.getSlot().split("-")[0]);
                    int endSlot = NumberUtils.toInt(clusterNodeObject.getSlot().split("-")[1]);
                    for (int i = beginSlot; i <= endSlot; i++) {
                        masterSlaveNode.addSlot(i);
                    }
                }
                //如果之前循环有从节点先出现则此map有值
                Set<String> slaveSet = tempMap.get(clusterNodeObject.getNodeId());
                if (slaveSet == null) {
                    tempMap.put(clusterNodeObject.getNodeId(), new HashSet<String>());
                } else if (!slaveSet.isEmpty()) {
                    masterSlaveNode.addAllSlaveHostAndPort(slaveSet);
                }
            } else if (useSlave) {
                String masterHostAndPort = tempMap2.get(clusterNodeObject.getMasterId());
                if (StringUtils.isBlank(masterHostAndPort)) {
                    Set<String> set = tempMap.get(clusterNodeObject.getMasterId());
                    if (set == null) {
                        set = new HashSet<String>();
                        tempMap.put(clusterNodeObject.getMasterId(), set);
                    }
                    set.add(clusterNodeObject.getHostAndPort());
                } else {
                    nodes.get(masterHostAndPort).addSlaveHostAndPort(clusterNodeObject.getHostAndPort());
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
            MasterSlaveNode masterSlaveNode = entry.getValue();
            HostAndPort hostAndPort = masterSlaveNode.getHostAndPort();
            setupNodeIfNotExist(hostAndPort);
            assignSlotsToNode(entry.getValue().getSlotList(), hostAndPort);
        }
    }

    public void renewClusterSlots(Jedis jedis) {
        //If rediscovering is already in process - no need to start one more same rediscovering, just return
        if (!rediscovering) {
            w.lock();
            try {
                rediscovering = true;

                if (jedis != null && "pong".equals(jedis.ping())) {//防止jedis不为空时但连接失效
                    try {
                        slots.clear();
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
                        if (jedis == null) {
                            continue;
                        }
                        String result = jedis.ping();
                        if (!result.equalsIgnoreCase("pong")) {
                            continue;
                        }
                        slots.clear();
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
            if (existingPool == null) {
                existingPool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
                        connectionTimeout, soTimeout, password, 0, clientName, false, null, null, null);
                masterSlaveNode.setMaster(existingPool);
            }
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
            return existingPool;
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

    private void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode) {
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
            if (useSlave) {
                MasterSlaveNode masterSlaveNode = slots.get(slot);
                return masterSlaveNode.getSlaveByStrategy(MasterSlaveNode.SlaveStrategy.ROUND_ROBIN);
            } else {
                return getSlotWritePool(slot);
            }
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

    /**
     * master slave jedispool container
     */
    private final static class MasterSlaveNode {

        public enum SlaveStrategy {
            ROUND_ROBIN
        }

        private final AtomicInteger counter = new AtomicInteger();
        private JedisPool master;
        private List<JedisPool> slave;
        private final ReentrantReadWriteLock nodeLock = new ReentrantReadWriteLock();
        private final Lock readLock = nodeLock.readLock();
        private final Lock writeLock = nodeLock.writeLock();
        private String masterHostAndPort;
        private Set<String> slaveHostAndPort;
        private String masterNodeId;
        private Set<String> slaveNodeId;
        private List<Integer> slotList;
        private String configEpoch;

        public MasterSlaveNode(ClusterNodeObject clusterNodeObject) {
            this.masterHostAndPort = clusterNodeObject.getHostAndPort();
            this.masterNodeId = clusterNodeObject.getNodeId();
            slaveHostAndPort = new HashSet<String>();
            slotList = new ArrayList<Integer>();
            this.configEpoch = clusterNodeObject.getConfigEpoch();
        }

        public Set<String> getSlaveHostAndPort() {
            return slaveHostAndPort;
        }

        public JedisPool getMaster() {
            return master;
        }

        public void setMaster(JedisPool master) {
            this.master = master;
        }

        public List<JedisPool> getSlave() {
            return slave;
        }

        public void setSlave(List<JedisPool> slave) {
            this.slave = slave;
        }

        public String getConfigEpoch() {
            return configEpoch;
        }

        public ReentrantReadWriteLock getNodeLock() {
            return nodeLock;
        }

        public Lock getReadLock() {
            return readLock;
        }

        public Lock getWriteLock() {
            return writeLock;
        }

        public String getMasterHostAndPort() {
            return masterHostAndPort;
        }

        public Set<String> getSlaveNodeId() {
            return slaveNodeId;
        }

        public void setSlaveNodeId(Set<String> slaveNodeId) {
            this.slaveNodeId = slaveNodeId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof MasterSlaveNode) {
                MasterSlaveNode hp = (MasterSlaveNode) obj;
                return hp.getMasterHostAndPort().equals(this.getMasterHostAndPort());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.getMasterHostAndPort().hashCode();
        }

        public void destroy() {
            destroyMaster();
            destroySlave();
        }

        public void destroySlave() {
            if (slave != null) {
                for (JedisPool jedisPool : slave) {
                    jedisPool.destroy();
                }
            }
            slave = null;
            slaveHostAndPort = new HashSet<String>();
        }

        public void destroyMaster() {
            if (master != null) {
                master.destroy();
            }
        }

        public void addSlot(int slot) {
            slotList.add(slot);
        }

        public void addAllSlaveHostAndPort(Collection<? extends String> data) {
            slaveHostAndPort.addAll(data);
        }

        public boolean addSlaveHostAndPort(String data) {
            writeLock.lock();
            try {
                return slaveHostAndPort.add(data);
            } finally {
                writeLock.unlock();
            }
        }

        public List<Integer> getSlotList() {
            return new ArrayList<Integer>(slotList);
        }

        public JedisPool getSlaveByStrategy(MasterSlaveNode.SlaveStrategy strategy) {
            JedisPool jedisPool = null;
            switch (strategy) {
                case ROUND_ROBIN:
                    jedisPool = roundRobinSlavePool();
                    break;
                default:
                    jedisPool = master;
            }
            if (jedisPool == null) {
                jedisPool = master;
            }
            return jedisPool;
        }

        public void clearSlot() {
            if (slotList != null) {
                slotList.clear();
            }
        }

        public HostAndPort getHostAndPort() {
            String host = masterHostAndPort.split(":")[0];
            int port = NumberUtils.toInt(masterHostAndPort.split(":")[1]);
            HostAndPort hostAndPort = new HostAndPort(host, port);
            return hostAndPort;
        }

        private JedisPool roundRobinSlavePool() {
            JedisPool jedisPool = null;
            if (slave != null && !slave.isEmpty()) {
                jedisPool = slave.get(counter.getAndIncrement() % slave.size());
            }
            return jedisPool;
        }
    }


    //sentinel listener
    private class CachePubSub extends JedisPubSub {

        @Override
        public void onMessage(String channel, String message) {
            switch (channel) {
                case SentinelEvents.SLAVE_PLUS:

                    addSlave2Node(message);
                    break;
                case SentinelEvents.SWITCH_MASTER:

                    break;
                case SentinelEvents.ODOWN_PLUS:

                    break;
                case SentinelEvents.SDOWN_PLUS:
                    removeNode(message);
                    break;
                case SentinelEvents.SDOWN_MINUS:
                    addNode(message);
                    break;
                default:
            }
        }


        private void addNode(String message) {
            for(;;){
                break;
            }
        }

        private void removeNode(String message) {
            for(;;){
                break;
            }
        }

        private void addSlave2Node(String message) {
            if (useSlave) {
                for (; ; ) {
                    if (!rediscovering) {
                        try {
                            rediscovering = true;
                            SentinelEvents.MessageDetail messageDetail = SentinelEvents.convertStr2MessageDetail(message);
                            HostAndPort master = new HostAndPort(messageDetail.masterIp, NumberUtils.toInt(messageDetail.masterPort));
                            HostAndPort slave = new HostAndPort(messageDetail.ip, NumberUtils.toInt(messageDetail.port));
                            MasterSlaveNode node = nodes.get(master.toString());
                            //主为空的情况先不管
                            if (node != null) {
                                if (node.addSlaveHostAndPort(slave.toString())) {
                                    JedisPool slavePool = new JedisPool(poolConfig, slave.getHost(), slave.getPort(),
                                            connectionTimeout, soTimeout, password, 0, clientName, false, null, null, null, false);
                                    node.getSlave().add(slavePool);
                                }
                            }
                            break;
                        } finally {
                            rediscovering = false;
                        }
                    }
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }


}
