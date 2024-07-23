package com.tosan.tools.lockmanager.impl.hazelcast;

import com.tosan.tools.lockmanager.api.LockManagementService;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author R.Mehri
 * @since 29/09/2020
 */
public class HazelcastLockManagementService implements LockManagementService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastLockManagementService.class);
    private final HazelcastService hazelcastLock;

    public HazelcastLockManagementService(HazelcastService hazelcastLock) {
        this.hazelcastLock = hazelcastLock;
    }

    @Override
    public void requestReadLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            hazelcastLock.requestReadLock(lockNameType, null, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            hazelcastLock.requestReadLock(lockNameType, lockName, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            hazelcastLock.requestReadLock(lockNameType, lockName, lockTimeout, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            hazelcastLock.requestWriteLock(lockNameType, null, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            hazelcastLock.requestWriteLock(lockNameType, lockName, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            hazelcastLock.requestWriteLock(lockNameType, lockName, lockTimeout, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void unlock(String lockNameType) throws LockManagerTimeoutException {
        try {
            hazelcastLock.unLock(lockNameType, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Could not unlock! {} " + e.getMessage(), lockNameType);
        }
    }

    @Override
    public void unlock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            hazelcastLock.unLock(lockNameType, lockName);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Could not unlock! {} " + e.getMessage(), lockName);
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToReadLock(String lockNameType) throws LockManagerTimeoutException {
        try {
            hazelcastLock.convertToReadLock(lockNameType, null, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToReadLock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            hazelcastLock.convertToReadLock(lockNameType, lockName, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToReadLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        try {
            hazelcastLock.convertToReadLock(lockNameType, lockName, lockTimeout);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToWriteLock(String lockNameType) throws LockManagerTimeoutException {
        try {
            hazelcastLock.convertToWriteLock(lockNameType, null, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToWriteLock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            hazelcastLock.convertToWriteLock(lockNameType, lockName, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @param lockTimeout  Number of seconds to continue trying to grant the lock.
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToWriteLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        try {
            hazelcastLock.convertToWriteLock(lockNameType, lockName, lockTimeout);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }
}
