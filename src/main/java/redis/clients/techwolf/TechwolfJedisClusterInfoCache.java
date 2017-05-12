package redis.clients.techwolf;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Created by zhaoyalong on 17-3-28.
 */
public class TechwolfJedisClusterInfoCache {

    private Logger log = Logger.getLogger(getClass().getName());

    private volatile Set<String> badNode = new HashSet<String>();

    private Map<String, MasterSlaveNode> nodes = new HashMap<String, MasterSlaveNode>();
    private Map<Integer, MasterSlaveNode> slots = new HashMap<Integer, MasterSlaveNode>();
    private static final int MASTER_NODE_INDEX = 2;
    private static final int DELAY_TIME = 3;
    private static volatile long lastRenewTime = 0;
    private AtomicBoolean lock = new AtomicBoolean(true);
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "clearOldNodeThread");
                }
            });

    private volatile boolean flag = true;
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private final ReentrantLock rediscoveringLock = new ReentrantLock();
    private final GenericObjectPoolConfig poolConfig;

    private int connectionTimeout;
    private int soTimeout;
    private String password;
    private String clientName;
    private boolean useSlave;

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
    }

    private void discoverClusterMasterAndSlave(Jedis jedis) {
        List<Object> slotsObject = jedis.clusterSlots();
        for (Object slotObj : slotsObject) {
            List<Object> slotInfo = (List<Object>) slotObj;
            if (slotInfo.size() <= MASTER_NODE_INDEX) {
                continue;
            }
            List<Integer> slotNums = getAssignedSlotArray(slotInfo);
            int size = slotInfo.size();
            MasterSlaveNode masterSlaveNode = null;
            for (int i = MASTER_NODE_INDEX; i < size; i++) {
                List<Object> hostInfos = (List<Object>) slotInfo.get(i);
                if (hostInfos.size() <= 0) {
                    continue;
                }
                HostAndPort targetNode = generateHostAndPort(hostInfos);
                String nodeId = SafeEncoder.encode((byte[]) hostInfos.get(2));
                //第一个是主
                if (i == MASTER_NODE_INDEX) {
                    masterSlaveNode = nodes.get(targetNode.toString());
                    if (masterSlaveNode == null) {
                        masterSlaveNode = new MasterSlaveNode(targetNode, nodeId);
                        nodes.put(targetNode.toString(), masterSlaveNode);
                    }
                } else {//从
                    if (useSlave) {
                        masterSlaveNode.getSlaveHostAndPort().add(targetNode.toString());
                        masterSlaveNode.getSlaveNodeId().add(nodeId);
                    }
                }
            }
            masterSlaveNode.getSlotList().addAll(slotNums);
        }
    }

    private void discoverClusterMasterAndSlave(Jedis jedis, String errorHost, int errorPort) {
        //等待次数越多时间就可以越小
        int waitTimes = 30;
        a:
        for (; waitTimes >= 0; waitTimes--) {
            List<Object> slotsObject = jedis.clusterSlots();
            for (Object slotObj : slotsObject) {
                List<Object> slotInfo = (List<Object>) slotObj;
                if (slotInfo.size() <= MASTER_NODE_INDEX) {
                    continue;
                }
                List<Integer> slotNums = getAssignedSlotArray(slotInfo);
                int size = slotInfo.size();
                MasterSlaveNode masterSlaveNode = null;
                for (int i = MASTER_NODE_INDEX; i < size; i++) {
                    List<Object> hostInfos = (List<Object>) slotInfo.get(i);
                    if (hostInfos.size() <= 0) {
                        continue;
                    }
                    HostAndPort targetNode = generateHostAndPort(hostInfos);
                    String nodeId = SafeEncoder.encode((byte[]) hostInfos.get(2));
                    //第一个是主
                    if (i == MASTER_NODE_INDEX) {
                        //现在主要考虑了主的情况
                        if (targetNode.toString().equals(errorHost + ":" + errorPort) && waitTimes > 0) {
                            //如果取出来的信息里面还有报错的主机　等待一会重新获取　因为节点刚出错重建时failover还没完成
                            //旧的节点信息还是不对
                            System.out.println("get error node keep wait :" + waitTimes);
                            nodes.clear();
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            continue a;
                        }
                        masterSlaveNode = nodes.get(targetNode.toString());
                        if (masterSlaveNode == null) {
                            masterSlaveNode = new MasterSlaveNode(targetNode, nodeId);
                            nodes.put(targetNode.toString(), masterSlaveNode);
                        }
                    } else {//从
                        if (useSlave) {
                            masterSlaveNode.getSlaveHostAndPort().add(targetNode.toString());
                            masterSlaveNode.getSlaveNodeId().add(nodeId);
                        }
                    }
                }
                masterSlaveNode.getSlotList().addAll(slotNums);
            }
            return;
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
            assignSlotsToNode(entry.getValue().getSlotListView(), hostAndPort);
        }
    }

    public void renewClusterSlots(Jedis jedis) {
        //If rediscovering is already in process - no need to start one more same rediscovering, just return
        System.out.println(new Date() + " " + Thread.currentThread().getName() + " in ");
        if (flag) {
//        if (rediscoveringLock.tryLock()) {
            if (lock.compareAndSet(true, false)) {
                flag = false;
                System.out.println(new Date() + " " + Thread.currentThread().getName() + " in　lock ");
                try {
                    long start = System.currentTimeMillis();
                    if (start - lastRenewTime < 10000) {
                        System.out.println(new Date() + " " + Thread.currentThread().getName() + " return ");
                        return;
                    }
                    System.out.println(new Date() + " " + Thread.currentThread().getName() + "renewClusterSlots begin-----------------------------");
                    TechwolfJedisClusterInfoCache reliefCache = new TechwolfJedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password, clientName, false);
                    //防止jedis不为空时但连接失效
                    if (jedis != null && "pong".equalsIgnoreCase(jedis.ping())) {
                        try {
                            reliefCache.discoverClusterMasterAndSlave(jedis);
                            reliefCache.setupAllMasterAndSlaveNode();
                        } catch (JedisException e) {
                            //try nodes from all pools
                        }
                    } else {
                        for (JedisPool jp : getShuffledNodesPool()) {
                            try {
                                jedis = jp.getResource();
                                if (jedis == null) {
                                    continue;
                                }
                                String result = jedis.ping();

                                if (!"pong".equalsIgnoreCase(result)) {
                                    continue;
                                }
                                reliefCache.discoverClusterMasterAndSlave(jedis);
                                reliefCache.setupAllMasterAndSlaveNode();
                                break;
                            } catch (JedisConnectionException e) {
                                // try next nodes
                            } finally {
                                if (jedis != null) {
                                    jedis.close();
                                }
                            }
                        }
                    }
                    final Map<String, MasterSlaveNode> tmpNodes = this.nodes;
                    w.lock();
                    try {
                        this.nodes = reliefCache.getNode();
                        this.slots = reliefCache.getSlot();
                    } finally {
                        w.unlock();
                    }
                    lastRenewTime = System.currentTimeMillis();
                    System.out.println(new Date() + " " + Thread.currentThread().getName() + "renewClusterSlots---------- time" + (lastRenewTime - start));
                    //延迟处理
                    if (tmpNodes != null) {
                        System.out.println(new Date() + " " + Thread.currentThread().getName() + tmpNodes);
                        this.scheduledExecutorService.schedule(new Runnable() {
                            @Override
                            public void run() {
                                for (MasterSlaveNode masterSlaveNode : tmpNodes.values()) {
                                    masterSlaveNode.destroy();
                                }
                            }
                        }, DELAY_TIME, TimeUnit.SECONDS);
                    }
                } finally {
//                rediscoveringLock.unlock();
                    flag = true;
                    lock.set(true);
                }
            }
            System.out.println(new Date() + " " + Thread.currentThread().getName() + " out ");
        }
    }

    public void renewClusterSlots(String host, int port) {
        //If rediscovering is already in process - no need to start one more same rediscovering, just return
        System.out.println(Thread.currentThread().getName() + " know node in ");
        if (rediscoveringLock.tryLock()) {
            System.out.println(Thread.currentThread().getName() + " know node  in　lock ");
            try {
                long start = System.currentTimeMillis();
                if (start - lastRenewTime < 10000) {
                    System.out.println(Thread.currentThread().getName() + " know node return ");
                    return;
                }
                badNode.add(host + ":" + port);
                System.out.println(Thread.currentThread().getName() + "renewClusterSlots begin-----------------------------");
                TechwolfJedisClusterInfoCache reliefCache = new TechwolfJedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password, clientName, false);
                //防止jedis不为空时但连接失效
                Jedis jedis = null;
                for (JedisPool jp : getShuffledNodesPool()) {
                    try {
                        jedis = jp.getResource();
                        if (jedis == null) {
                            continue;
                        }
                        String result = jedis.ping();

                        if (!"pong".equalsIgnoreCase(result)) {
                            continue;
                        }
                        reliefCache.discoverClusterMasterAndSlave(jedis, host, port);
                        reliefCache.setupAllMasterAndSlaveNode();
                        break;
                    } catch (JedisConnectionException e) {
                        // try next nodes
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }
                final Map<String, MasterSlaveNode> tmpNodes = this.nodes;
                w.lock();
                try {
                    this.nodes = reliefCache.getNode();
                    this.slots = reliefCache.getSlot();
//                    if (StringUtils.isNotBlank(host) && port > 0) {
//                        String errorKey = host + ":" + port;
//                        MasterSlaveNode errorNode = tmpNodes.get(errorKey);
//                        if (errorNode != null) {
//                            errorNode.destroy();
//                            tmpNodes.remove(errorKey);
//                            System.out.println(Thread.currentThread().getName() + "immediate destory error node" + errorKey);
//                        }
//                    }
                } finally {
                    w.unlock();
                }
                lastRenewTime = System.currentTimeMillis();
                System.out.println(Thread.currentThread().getName() + "renewClusterSlots---------- time" + (lastRenewTime - start));
                //延迟处理
                if (tmpNodes != null) {
                    System.out.println(Thread.currentThread().getName() + tmpNodes);
                    this.scheduledExecutorService.schedule(new Runnable() {
                        @Override
                        public void run() {
                            badNode.clear();
                            for (MasterSlaveNode masterSlaveNode : tmpNodes.values()) {
                                masterSlaveNode.destroy();
                            }
                        }
                    }, DELAY_TIME, TimeUnit.SECONDS);
                }
            } finally {
                rediscoveringLock.unlock();
            }
        }
        System.out.println(Thread.currentThread().getName() + " out ");
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

    public void renewSlotCache(Jedis connection, int slot, HostAndPort targetNode) {
        MasterSlaveNode newNode = nodes.get(targetNode.toString());
        if (newNode == null) {
            //TODO
        }
        w.lock();
        try {
            slots.put(slot, newNode);
        } finally {
            w.unlock();
        }
    }

    public boolean badJedis(String hostAndPort) {
        return badNode.contains(hostAndPort);
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

        public MasterSlaveNode(ClusterNodeObject clusterNodeObject) {
            this.masterHostAndPort = clusterNodeObject.getHostAndPort();
            this.masterNodeId = clusterNodeObject.getNodeId();
            slaveHostAndPort = new HashSet<String>();
            slotList = new ArrayList<Integer>();
            slaveNodeId = new HashSet<String>();
        }

        public MasterSlaveNode(HostAndPort hostAndPort, String nodeId) {
            this.masterHostAndPort = hostAndPort.toString();
            this.masterNodeId = nodeId;
            slaveHostAndPort = new HashSet<String>();
            slotList = new ArrayList<Integer>();
            slaveNodeId = new HashSet<String>();
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
            System.out.println(new Date() + " " + getMasterHostAndPort() + ":destory");
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

        public List<Integer> getSlotListView() {
            return new ArrayList<Integer>(slotList);
        }

        public List<Integer> getSlotList() {
            return slotList;
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
    @Deprecated
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
            for (; ; ) {
                break;
                //TODO
            }
        }

        private void removeNode(String message) {
            for (; ; ) {
                break;
                //TODO
            }
        }

        private void addSlave2Node(String message) {
            if (useSlave) {
                for (; ; ) {
                    if (rediscoveringLock.tryLock()) {
                        try {
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
                            rediscoveringLock.unlock();
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


    public Map<String, MasterSlaveNode> getNode() {
        return nodes;
    }

    public Map<Integer, MasterSlaveNode> getSlot() {
        return slots;
    }
}
