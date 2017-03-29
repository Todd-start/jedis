package redis.clients.techwolf;

import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zhaoyalong on 17-3-29.
 */
public class MasterSlaveNode {

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

    public void setMasterHostAndPort(String masterHostAndPort) {
        this.masterHostAndPort = masterHostAndPort;
    }

    public Set<String> getSlaveHostAndPort() {
        return slaveHostAndPort;
    }

    public void setSlaveHostAndPort(Set<String> slaveHostAndPort) {
        this.slaveHostAndPort = slaveHostAndPort;
    }

    public String getMasterNodeId() {
        return masterNodeId;
    }

    public void setMasterNodeId(String masterNodeId) {
        this.masterNodeId = masterNodeId;
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
        if (master != null) {
            master.destroy();
        }
        if (slave != null) {
            for (JedisPool jedisPool : slave) {
                jedisPool.destroy();
            }
        }
    }

    public List<Integer> getSlotList() {
        return slotList;
    }

    public void setSlotList(List<Integer> slotList) {
        this.slotList = slotList;
    }
}
