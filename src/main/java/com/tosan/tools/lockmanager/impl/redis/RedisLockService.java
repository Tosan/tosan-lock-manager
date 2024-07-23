package com.tosan.tools.lockmanager.impl.redis;

import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author R.Mehri
 * @since 29/09/2020
 */
public class RedisLockService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisLockService.class);
    private static final int DEFAULT_READ_LOCK_TIMEOUT = 60;
    private static final int DEFAULT_WRITE_LOCK_TIMEOUT = 7200;
    private RedissonClient redisClient;
    private int lockExpireSecond = 3600;

    public void setLockExpireSecond(int lockExpireSecond) {
        this.lockExpireSecond = lockExpireSecond;
    }

    public void setRedisClient(RedissonClient redisClient) {
        this.redisClient = redisClient;
    }

    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit)
            throws LockManagerTimeoutException {
        Integer timeout = lockTimeout;
        if (timeout == null) {
            timeout = DEFAULT_READ_LOCK_TIMEOUT;
        }
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting read lock with handle {}", lockHandle);
        try {
            RReadWriteLock readWriteLock = redisClient.getReadWriteLock(lockHandle);
            RLock rLock = readWriteLock.readLock();
            boolean granted = rLock.tryLock(timeout, lockExpireSecond, TimeUnit.SECONDS);
            if (!granted) {
                throw new LockManagerTimeoutException("Timeout error occurred in 'REDIS_LOCK' request.");
            }
            LOGGER.debug("Acquired read lock with handle {}.", lockHandle);
        } catch (InterruptedException e) {
            throw new LockManagerTimeoutException("Timeout error occurred in 'REDIS_LOCK' request.");
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
            RReadWriteLock readWriteLock = redisClient.getReadWriteLock(lockHandle);
            RLock rLock = readWriteLock.writeLock();
            boolean granted = rLock.tryLock(timeout, lockExpireSecond, TimeUnit.SECONDS);
            if (!granted) {
                throw new LockManagerTimeoutException("Timeout error occurred in 'REDIS_LOCK' request.");
            }
            LOGGER.debug("Acquired write lock with handle {}.", lockHandle);
        } catch (InterruptedException e) {
            throw new LockManagerTimeoutException("Timeout error occurred in 'REDIS_LOCK' request.");
        }
    }

    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting convert to read lock with handle {}", lockHandle);
        RReadWriteLock readWriteLock = redisClient.getReadWriteLock(lockHandle);
        RLock wLock = readWriteLock.writeLock();
        RLock rLock = readWriteLock.readLock();
        if (rLock.isLocked()) {
            LOGGER.debug("Allready granted a read lock with handle {}", lockHandle);
        } else {
            RLock convertLock = redisClient.getLock(lockHandle);
            try {
                boolean convertLockGranted = convertLock.tryLock(0, TimeUnit.SECONDS);
                if (convertLockGranted) {
                    throw new LockManagerTimeoutException("Another thread is converting this lock!");
                } else {
                    wLock.unlock();
                    requestReadLock(lockNameType, lockName, null, false);
                    convertLock.unlock();
                    LOGGER.debug("Converted to read lock with handle {}", lockHandle);
                }
            } catch (InterruptedException | IllegalMonitorStateException e) {
                throw new LockManagerTimeoutException("Timeout error occurred in 'REDIS_LOCK' convert.");
            }
        }
    }

    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
        String lockHandle = getLockHandle(lockNameType, lockName);
        LOGGER.debug("Requesting convert to write lock with handle {}", lockHandle);
        RReadWriteLock readWriteLock = redisClient.getReadWriteLock(lockHandle);
        RLock wLock = readWriteLock.writeLock();
        RLock rLock = readWriteLock.readLock();
        if (wLock.isLocked()) {
            LOGGER.debug("Already granted a write lock with handle {}", lockHandle);
        } else {
            RLock convertLock = redisClient.getLock(lockHandle);
            try {
                boolean convertLockGranted = convertLock.tryLock(0, TimeUnit.SECONDS);
                if (!convertLockGranted) {
                    throw new LockManagerTimeoutException("Another thread is converting this lock!");
                } else {
                    rLock.unlock();
                    requestWriteLock(lockNameType, lockName, null, false);
                    convertLock.unlock();
                    LOGGER.debug("Converted to write lock with handle {}", lockHandle);
                }
            } catch (InterruptedException | IllegalMonitorStateException e) {
                throw new LockManagerTimeoutException("Timeout error occurred in 'REDIS_LOCK' convert.");
            }
        }
    }

    public void unLock(String lockNameType, String lockName) {
        try {
            String lockHandle = getLockHandle(lockNameType, lockName);
            LOGGER.debug("Requesting release lock with handle {}", lockHandle);
            RReadWriteLock readWriteLock = redisClient.getReadWriteLock(lockHandle);
            RLock rLock = readWriteLock.readLock();
            RLock wLock = readWriteLock.writeLock();
            if (rLock.isLocked()) {
                rLock.unlock();
            }
            if (wLock.isLocked()) {
                wLock.unlock();
            }
            LOGGER.debug("Released lock with handle {}", lockHandle);
        } catch (IllegalMonitorStateException e) {
            LOGGER.debug("Current thread is not owner of lock");
        }
    }

    private String getLockHandle(String lockNameType, String lockName) {
        LOGGER.debug("Requesting lock handle for lock with name '{}'.", lockName);
        return lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
    }
}
