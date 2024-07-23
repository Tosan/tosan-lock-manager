package com.tosan.tools.lockmanager.impl.zookeeper;

import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Hajihosseinkhani
 * @since 27/06/2021
 **/

public class ZookeeperLockService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLockService.class);

    private final CuratorFramework client;
    private final Map<String, InterProcessReadWriteLock> interProcessReadWriteLockMapByName = new HashMap<>();

    public ZookeeperLockService(CuratorFramework client) {
        this.client = client;
    }

    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        InterProcessReadWriteLock readWriteLock = getReadWriteLockInstance(lockNameType, lockName);
        boolean granted;
        try {
            granted = lockTimeout != null ?
                    readWriteLock.readLock().acquire(lockTimeout, TimeUnit.SECONDS) :
                    readWriteLock.readLock().acquire(-1, null);
        } catch (Exception exception) {
            throw new LockManagerTimeoutException("Timeout occurred in ZOOKEEPER_LOCK request.");
        }
        if (!granted) {
            throw new LockManagerTimeoutException("Read lock is held by another process.");
        }
        LOGGER.debug("Read lock granted for path {}", getLockPath(lockNameType, lockName));

    }


    public void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout) {
        System.out.print(Thread.currentThread().getName() + "enter into write lock.");
        InterProcessReadWriteLock readWriteLock = getReadWriteLockInstance(lockNameType, lockName);
        boolean granted;
        try {
            granted = lockTimeout != null ?
                    readWriteLock.writeLock().acquire(lockTimeout, TimeUnit.SECONDS) :
                    readWriteLock.writeLock().acquire(-1, null);
        } catch (Exception exception) {
            throw new LockManagerTimeoutException("Timeout occurred in ZOOKEEPER_LOCK request.");
        }
        if (!granted) {
            throw new LockManagerTimeoutException("Write lock is held by another process.");
        }
        LOGGER.debug("Write lock granted for path {}", getLockPath(lockNameType, lockName));

    }

    public void unlock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        InterProcessReadWriteLock readWriteLock = getReadWriteLockInstance(lockNameType, lockName);
        try {
            if (readWriteLock != null && readWriteLock.readLock().isOwnedByCurrentThread()) {
                readWriteLock.readLock().release();
            } else if (readWriteLock != null && readWriteLock.writeLock().isOwnedByCurrentThread()) {
                readWriteLock.writeLock().release();
            }
        } catch (Exception exception) {
            LOGGER.info("Current thread does not own the lock for path {}", getLockPath(lockNameType, lockName));
        }
    }


    public void convertToReadLock(String lockNameType, String lockName, Integer lockTimeout) {
        InterProcessReadWriteLock readWriteLock = getReadWriteLockInstance(lockNameType, lockName);
        if (readWriteLock.writeLock().isOwnedByCurrentThread()) {
            unlock(lockNameType, lockName);
            requestReadLock(lockNameType, lockName, lockTimeout);
        } else {
            throw new LockManagerTimeoutException("Thread does not own write lock to convert.");
        }
    }

    public void convertToWriteLock(String lockNameType, String lockName, Integer lockTimeout) {
        InterProcessReadWriteLock readWriteLock = getReadWriteLockInstance(lockNameType, lockName);
        if (readWriteLock != null && readWriteLock.readLock().isOwnedByCurrentThread()) {
            unlock(lockNameType, lockName);
            requestWriteLock(lockNameType, lockName, lockTimeout);
        } else {
            throw new LockManagerTimeoutException("Thread does not own any read lock to convert.");
        }
    }

    private InterProcessReadWriteLock getReadWriteLockInstance(String lockNameType, String lockName) {
        interProcessReadWriteLockMapByName.putIfAbsent(getLockPath(lockNameType, lockName),
                new InterProcessReadWriteLock(client, getLockPath(lockNameType, lockName)));
        return interProcessReadWriteLockMapByName.get(getLockPath(lockNameType, lockName));
    }

    private String getLockPath(String lockNameType, String lockName) {
        return "/" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : null);
    }
}
