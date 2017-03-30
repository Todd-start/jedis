package redis.clients.techwolf;

import org.apache.commons.lang3.math.NumberUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhaoyalong on 17-3-29.
 */
public class MasterSlaveNode {


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

    public void addSlaveHostAndPort(String data) {
        slaveHostAndPort.add(data);
    }

    public List<Integer> getSlotList() {
        return new ArrayList<Integer>(slotList);
    }

    public JedisPool getSlaveByStrategy(SlaveStrategy strategy) {
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
