package com.tosan.tools.lockmanager.impl.hazelcast;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.IMap;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

/**
 * @author R.Mehri
 * @since 29/09/2020
 */
public class HazelcastService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastService.class);
    private static final int DEFAULT_READ_LOCK_TIMEOUT = 60;
    private static final int DEFAULT_WRITE_LOCK_TIMEOUT = 7200;
    private static final String READ_LOCKS_MAP_NAME = "readLocks";
    private static final String WRITE_LOCKS_MAP_NAME = "writeLocks";
    private static final String DOWN_MEMBERS_MAP_NAME = "downMembers";
    private static final String READ_LOCK_ACCESS_MAP_NAME = "readLockAccess";
    private static final String WRITE_LOCK_ACCESS_MAP_NAME = "writeLockAccess";

    private HazelcastInstance hazelcastInstance;
    private IMap<String, HazelcastWriteLockInfo> writeLocks;
    private IMap<String, HazelcastReadLockInfo> readLocks;
    private IMap<String, Boolean> readLockAccess;
    private IMap<String, Boolean> writeLockAccess;

    private int lockExpireSecs;
    private String splitBrainConfigName;
    private String lockClusterDownMembersSetName;
    private Set<String> downMembers;

    public void setLockClusterDownMembersSetName(String lockClusterDownMembersSetName) {
        this.lockClusterDownMembersSetName = lockClusterDownMembersSetName;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        setSplitBrainConfigs();
        getSharedMaps();
        hazelcastInstance.getCluster().addMembershipListener(new ClusterMembershipListener());
    }

    private void setSplitBrainConfigs() {
        MapConfig readLockConfig = new MapConfig();
        readLockConfig.setSplitBrainProtectionName(splitBrainConfigName);
        readLockConfig.setName(READ_LOCKS_MAP_NAME);
        hazelcastInstance.getConfig().addMapConfig(readLockConfig);
        MapConfig writeLockConfig = new MapConfig();
        writeLockConfig.setSplitBrainProtectionName(splitBrainConfigName);
        writeLockConfig.setName(WRITE_LOCKS_MAP_NAME);
        MapConfig readLockAccessLockConfig = new MapConfig();
        readLockAccessLockConfig.setSplitBrainProtectionName(splitBrainConfigName);
        readLockAccessLockConfig.setName(READ_LOCK_ACCESS_MAP_NAME);
        MapConfig writeLockAccessLockConfig = new MapConfig();
        writeLockAccessLockConfig.setSplitBrainProtectionName(splitBrainConfigName);
        writeLockAccessLockConfig.setName(WRITE_LOCKS_MAP_NAME);
        hazelcastInstance.getConfig().addMapConfig(readLockConfig);
        hazelcastInstance.getConfig().addMapConfig(writeLockConfig);
        hazelcastInstance.getConfig().addMapConfig(readLockAccessLockConfig);
        hazelcastInstance.getConfig().addMapConfig(writeLockAccessLockConfig);
    }

    private void getSharedMaps() {
        writeLocks = hazelcastInstance.getMap(WRITE_LOCKS_MAP_NAME);
        readLocks = hazelcastInstance.getMap(READ_LOCKS_MAP_NAME);
        //downMembers = hazelcastInstance.getSet(DOWN_MEMBERS_MAP_NAME);
        readLockAccess = hazelcastInstance.getMap(READ_LOCK_ACCESS_MAP_NAME);
        writeLockAccess = hazelcastInstance.getMap(WRITE_LOCK_ACCESS_MAP_NAME);
        downMembers = hazelcastInstance.getSet(lockClusterDownMembersSetName);
    }

    public void setSplitBrainConfigName(String splitBrainConfigName) {
        this.splitBrainConfigName = splitBrainConfigName;
    }

    public void setLockExpireSecs(String lockExpireSecs) {
        this.lockExpireSecs = parseInt(lockExpireSecs);
    }

    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) {
        Integer timeout = lockTimeout;
        if (timeout == null) {
            timeout = DEFAULT_READ_LOCK_TIMEOUT;
        }
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting read lock with handle {}", lockHandle);
        try {
            if (writeLockAccess.tryLock(lockHandle, timeout, TimeUnit.SECONDS)) {
                getReadLock(lockHandle);
                writeLockAccess.unlock(lockHandle);
                LOGGER.debug("Acquired read lock with handle {}.", lockHandle);
            } else {
                throw new LockManagerTimeoutException("Timeout error occurred in 'HAZELCAST_LOCK' request.");
            }
        } catch (InterruptedException e) {
            throw new LockManagerTimeoutException("Timeout error occurred in 'HAZELCAST_LOCK' request.");
        } catch (SplitBrainProtectionException e) {
            LOGGER.debug("Minimum number of cluster nodes is required for using lock services!");
            throw new LockManagerRunTimeException("Minimum number of cluster nodes is required for using lock services!");
        } catch (HazelcastInstanceNotActiveException e) {
            throw new LockManagerRunTimeException("Hazelcast instance is not active");
        }
    }

    public void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) {
        Integer timeout = lockTimeout;
        if (timeout == null) {
            timeout = DEFAULT_WRITE_LOCK_TIMEOUT;
        }
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting write lock with handle {}", lockHandle);
        try {
            if (writeLockAccess.tryLock(lockHandle, timeout, TimeUnit.SECONDS)) {
                if (readLockAccess.tryLock(lockHandle, timeout, TimeUnit.SECONDS)) {
                    getWriteLock(lockHandle);
                    LOGGER.debug("Acquired write lock with handle {}.", lockHandle);
                    writeLockAccess.put(lockHandle, true);
                } else {
                    writeLockAccess.unlock(lockHandle);
                    throw new LockManagerTimeoutException("Timeout error occurred in 'HAZELCAST_LOCK' request.");
                }
            } else {
                throw new LockManagerTimeoutException("Timeout error occurred in 'HAZELCAST_LOCK' request.");
            }
        } catch (InterruptedException e) {
            throw new LockManagerTimeoutException("Timeout error occurred in 'HAZELCAST_LOCK' request.");
        } catch (SplitBrainProtectionException e) {
            LOGGER.debug("Minimum number of cluster nodes is required for using lock services!");
            throw new LockManagerRunTimeException("Minimum number of cluster nodes is required for using lock services!");
        } catch (HazelcastInstanceNotActiveException e) {
            throw new LockManagerRunTimeException("Hazelcast instance is not active");
        }
    }

    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting convert to read lock with handle {}", lockHandle);
        if (writeLockAccess.tryLock(lockHandle) && readLockAccess.tryLock(lockHandle)) {
            HazelcastReadLockInfo readLockInfo = readLocks.get(lockHandle);
            if (readLockInfo != null) {
                LOGGER.debug("Allready granted a read lock with handle {}", lockHandle);
            } else {
                unLockWriteLock(lockHandle);
                getReadLock(lockHandle);
                LOGGER.debug("Converted to read lock with handle {}", lockHandle);
            }
            writeLockAccess.forceUnlock(lockHandle);
        } else {
            throw new LockManagerTimeoutException("Timeout error occurred in 'HAZELCAST_LOCK' request.");
        }
    }

    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting convert to write lock with handle {}", lockHandle);
        if (writeLockAccess.tryLock(lockHandle) && readLockAccess.tryLock(lockHandle)) {
            HazelcastWriteLockInfo writeLockInfo = writeLocks.get(lockHandle);
            if (writeLockInfo != null) {
                LOGGER.debug("Allready granted a read lock with handle {}", lockHandle);
            } else {
                HazelcastReadLockInfo readLockInfo = readLocks.get(lockHandle);
                if (readLockInfo.getSize() == 1) {
                    unLockReadLock(lockHandle);
                    getWriteLock(lockHandle);
                    LOGGER.debug("Converted to write lock with handle {}", lockHandle);
                } else {
                    writeLockAccess.forceUnlock(lockHandle);
                }
            }
        }
    }

    public void unLock(String lockNameType, String lockName) {
        try {
            String lockHandle = getLockHandle(lockNameType, lockName);
            LOGGER.debug("Requesting release lock with handle {}", lockHandle);
            if (writeLockAccess.tryLock(lockHandle) && readLockAccess.tryLock(lockHandle)) {
                if (writeLocks.get(lockHandle) != null && writeLockAccess.get(lockHandle)) {
                    unLockWriteLock(lockHandle);
                }
                if (readLocks.get(lockHandle) != null && readLocks.get(lockHandle).getSize() != 0) {
                    unLockReadLock(lockHandle);
                }
                readLockAccess.forceUnlock(lockHandle);
                writeLockAccess.forceUnlock(lockHandle);
            }
            LOGGER.debug("Released lock with handle {}", lockHandle);
        } catch (IllegalMonitorStateException e) {
            LOGGER.debug("Current thread is not owner of lock");
        } catch (SplitBrainProtectionException e) {
            LOGGER.debug("Minimum number of cluster nodes is required for using lock services!");
            throw new LockManagerRunTimeException("Minimum number of cluster nodes is required for using lock services!");
        } catch (HazelcastInstanceNotActiveException e) {
            throw new LockManagerRunTimeException("Hazelcast instance is not active");
        }
    }

    private void unLockWriteLock(String lockHandle) {
        writeLocks.remove(lockHandle);
        writeLockAccess.remove(lockHandle);
    }

    private void unLockReadLock(String lockHandle) {
        HazelcastReadLockInfo readLockInfo = readLocks.get(lockHandle);
        readLockInfo.removeMember(getMemberId());
        readLocks.replace(lockHandle, readLockInfo);
    }

    private void getReadLock(String lockHandle) {
        HazelcastReadLockInfo readLockInfo = readLocks.get(lockHandle);
        checkWriteLockAccess(lockHandle);
        boolean isReadLockInfoNull = true;
        if (readLockInfo != null) {
            isReadLockInfoNull = false;
        } else {
            Calendar expireDate = Calendar.getInstance();
            expireDate.set(Calendar.SECOND, expireDate.get(Calendar.SECOND) + lockExpireSecs);
            readLockInfo = new HazelcastReadLockInfo(lockHandle, expireDate.getTime());
        }
        readLockInfo.addMember(getMemberId());
        if (isReadLockInfoNull) {
            readLocks.put(lockHandle, readLockInfo);
        } else {
            readLocks.replace(lockHandle, readLockInfo);
        }
        readLockAccess.tryLock(lockHandle);
    }

    private void getWriteLock(String lockHandle) {
        checkWriteLockAccess(lockHandle);
        checkReadLockAccess(lockHandle);
        Calendar expireDate = Calendar.getInstance();
        expireDate.set(Calendar.SECOND, expireDate.get(Calendar.SECOND) + lockExpireSecs);
        HazelcastWriteLockInfo writeLockInfo = new HazelcastWriteLockInfo(lockHandle, getMemberId(), expireDate.getTime());
        writeLocks.put(lockHandle, writeLockInfo);
    }

    private void checkWriteLockAccess(String lockHandle) {
        HazelcastWriteLockInfo writeLockInfo = writeLocks.get(lockHandle);
        if (writeLockInfo != null) {
            if (!removeDownMembersWriteLocks(writeLockInfo)) {
                throw new LockManagerTimeoutException("Timeout error occurred in 'DBMS_LOCK' request.");
            }
        }
    }

    private void checkReadLockAccess(String lockHandle) {
        HazelcastReadLockInfo readLockInfo = readLocks.get(lockHandle);
        if (readLockInfo != null) {
            if (!removeDownMembersReadLocks(readLockInfo)) {
                throw new LockManagerTimeoutException("Timeout error occurred in 'DBMS_LOCK' request.");
            }
        }
    }

    private boolean removeDownMembersReadLocks(HazelcastReadLockInfo readLockInfo) {
        readLockInfo.removeMembers(downMembers);
        if (readLockInfo.getSize() == 0 || readLockInfo.isExpire()) {
            readLocks.remove(readLockInfo.getLockHandle());
            return true;
        }
        return false;
    }

    private boolean removeDownMembersWriteLocks(HazelcastWriteLockInfo writeLockInfo) {
        if (downMembers.contains(writeLockInfo.getMember()) || writeLockInfo.isExpire()) {
            writeLocks.remove(writeLockInfo.getLockHandle());
            return true;
        }
        return false;
    }

    private String getLockHandle(String lockNameType, String lockName) {
        LOGGER.debug("Requesting lock handle for lock with name '{}'.", lockName);
        return lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
    }

    private String getMemberId() {
        return hazelcastInstance.getLocalEndpoint().getUuid().toString();
    }

    private class ClusterMembershipListener implements MembershipListener {

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {

        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            downMembers.add(membershipEvent.getMember().getUuid().toString());
        }
    }
}
