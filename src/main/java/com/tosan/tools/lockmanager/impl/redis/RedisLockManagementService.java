package com.tosan.tools.lockmanager.impl.redis;

import com.tosan.tools.lockmanager.api.LockManagementService;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author R.Mehri
 * @since 29/09/2020
 */
public class RedisLockManagementService implements LockManagementService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisLockManagementService.class);
    private final RedisLockService redisLockService;

    public RedisLockManagementService(RedisLockService redisLockService) {
        this.redisLockService = redisLockService;
    }

    @Override
    public void requestReadLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            redisLockService.requestReadLock(lockNameType, null, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            redisLockService.requestReadLock(lockNameType, lockName, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            redisLockService.requestReadLock(lockNameType, lockName, lockTimeout, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            redisLockService.requestWriteLock(lockNameType, null, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            redisLockService.requestWriteLock(lockNameType, lockName, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            redisLockService.requestWriteLock(lockNameType, lockName, lockTimeout, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    @Override
    public void unlock(String lockNameType) throws LockManagerTimeoutException {
        try {
            redisLockService.unLock(lockNameType, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Could not unlock! {} " + e.getMessage(), lockNameType);
        }
    }

    @Override
    public void unlock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            redisLockService.unLock(lockNameType, lockName);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Could not unlock! {} " + e.getMessage(), lockName);
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException                                       If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToReadLock(String lockNameType) throws LockManagerTimeoutException {
        try {
            redisLockService.convertToReadLock(lockNameType, null, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockNameType);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException                                       If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToReadLock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            redisLockService.convertToReadLock(lockNameType, lockName, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException                                       If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToReadLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        try {
            redisLockService.convertToReadLock(lockNameType, lockName, lockTimeout);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException                                       If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToWriteLock(String lockNameType) throws LockManagerTimeoutException {
        try {
            redisLockService.convertToWriteLock(lockNameType, null, null);
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
     * @throws LockManagerTimeoutException                                       If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToWriteLock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            redisLockService.convertToWriteLock(lockNameType, lockName, null);
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
     * @throws LockManagerTimeoutException                                       If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    public void convertToWriteLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        try {
            redisLockService.convertToWriteLock(lockNameType, lockName, lockTimeout);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method: " + e.getMessage(), lockName);
            throw e;
        }
    }
}
