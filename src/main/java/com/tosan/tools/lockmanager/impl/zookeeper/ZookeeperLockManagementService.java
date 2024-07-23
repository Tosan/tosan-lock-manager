package com.tosan.tools.lockmanager.impl.zookeeper;

import com.tosan.tools.lockmanager.api.LockManagementService;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;

/**
 * @author Hajihosseinkhani
 * @since 27/06/2021
 **/
public class ZookeeperLockManagementService implements LockManagementService {
    private final ZookeeperLockService zookeeperLockService;

    public ZookeeperLockManagementService(ZookeeperLockService zookeeperLockService) {
        this.zookeeperLockService = zookeeperLockService;
    }

    @Override
    public void requestReadLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        zookeeperLockService.requestReadLock(lockNameType, null, null);
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        zookeeperLockService.requestReadLock(lockNameType, lockName, null);
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException {
        zookeeperLockService.requestReadLock(lockNameType, lockName, lockTimeout);
    }

    @Override
    public void requestWriteLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        zookeeperLockService.requestWriteLock(lockNameType, null, null);
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        zookeeperLockService.requestWriteLock(lockNameType, lockName, null);
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException {
        zookeeperLockService.requestWriteLock(lockNameType, lockName, lockTimeout);
    }

    @Override
    public void unlock(String lockNameType) {
        zookeeperLockService.unlock(lockNameType, null);
    }

    @Override
    public void unlock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        zookeeperLockService.unlock(lockNameType, lockName);
    }

    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        zookeeperLockService.convertToReadLock(lockNameType, lockName, lockTimeout);
    }

    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        zookeeperLockService.convertToWriteLock(lockNameType, lockName, lockTimeout);
    }
}
