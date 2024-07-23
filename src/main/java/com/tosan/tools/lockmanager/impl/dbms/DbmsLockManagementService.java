package com.tosan.tools.lockmanager.impl.dbms;

import com.tosan.tools.lockmanager.api.LockManagementService;
import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import com.tosan.tools.lockmanager.impl.dbms.dao.DbmsLockDao;
import com.tosan.tools.lockmanager.impl.dbms.dao.DbmsLockDaoFactory;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author akhbari
 * @since 23/02/2019
 */
public class DbmsLockManagementService implements LockManagementService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbmsLockManagementService.class);
    private final DbmsLockDao dbmsLockDao;

    public DbmsLockManagementService(DbmsLockDao dbmsLockDao) {
        this.dbmsLockDao = dbmsLockDao;
    }

    public DbmsLockManagementService(EntityManager entityManager) {
        dbmsLockDao = DbmsLockDaoFactory.getDbmsLockDao(entityManager);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void requestReadLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.requestReadLock(lockNameType, null, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method. {}", lockNameType, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void requestReadLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.requestReadLock(lockNameType, lockName, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit)
            throws LockManagerTimeoutException {
        try {
            dbmsLockDao.requestReadLock(lockNameType, lockName, lockTimeout, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void requestWriteLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.requestWriteLock(lockNameType, null, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method. {}", lockNameType, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void requestWriteLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.requestWriteLock(lockNameType, lockName, null, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit)
            throws LockManagerTimeoutException {
        try {
            dbmsLockDao.requestWriteLock(lockNameType, lockName, lockTimeout, releaseOnCommit);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method. {}", lockNameType, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void unlock(String lockNameType) {
        dbmsLockDao.unLock(lockNameType, null);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void unlock(String lockNameType, String lockName) {
        dbmsLockDao.unLock(lockNameType, lockName);
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void convertToReadLock(String lockNameType) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.convertToReadLock(lockNameType, null, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method. {}", lockNameType, e.getMessage());
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void convertToReadLock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.convertToReadLock(lockNameType, lockName, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.convertToReadLock(lockNameType, lockName, lockTimeout);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void convertToWriteLock(String lockNameType) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.convertToWriteLock(lockNameType, null, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method. {}", lockNameType, e.getMessage());
            throw e;
        }
    }

    /**
     * Converts a lock from one mode to another mode according to parameters
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void convertToWriteLock(String lockNameType, String lockName) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.convertToWriteLock(lockNameType, lockName, null);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException {
        try {
            dbmsLockDao.convertToWriteLock(lockNameType, lockName, lockTimeout);
        } catch (LockManagerTimeoutException e) {
            LOGGER.warn("Write or read lock '{}' held by another method. {}", lockName, e.getMessage());
            throw e;
        }
    }
}
