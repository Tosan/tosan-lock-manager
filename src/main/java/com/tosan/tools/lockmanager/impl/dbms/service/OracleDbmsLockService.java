package com.tosan.tools.lockmanager.impl.dbms.service;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.OracleDbmsLockDao;
import jakarta.persistence.EntityManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class OracleDbmsLockService implements DbmsLockService {
    private static final Logger logger = LoggerFactory.getLogger(OracleDbmsLockService.class);
    private static final short DBMS_LOCK_NAME_MAX_LENGTH = 128;
    private static final Map<String, DbmsLockInfo> dbmsLockHandle = new ConcurrentHashMap<>();
    private final OracleDbmsLockDao oracleDbmsLockDao;
    private boolean lockIdentifiersCache = false;
    private int allocatedLockTimeToLiveInSecond = 864000;
    private String schemaName = null;


    public OracleDbmsLockService(EntityManager entityManager) {
        this.oracleDbmsLockDao = new OracleDbmsLockDao(entityManager);
    }

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        this.lockIdentifiersCache = lockIdentifiersCache;
    }

    public void setAllocatedLockTimeToLiveInSecond(int allocatedLockTimeToLiveInSecond) {
        this.allocatedLockTimeToLiveInSecond = allocatedLockTimeToLiveInSecond;
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        oracleDbmsLockDao.requestLock(
                getLockHandle(getSchemaName(), lockNameType, lockName),
                OracleDbmsLockDao.SUB_SHARED_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        oracleDbmsLockDao.requestLock(
                getLockHandle(getSchemaName(), lockNameType, lockName),
                OracleDbmsLockDao.EXCLUSIVE_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
        oracleDbmsLockDao.convertLock(
                getLockHandle(getSchemaName(), lockNameType, lockName),
                OracleDbmsLockDao.SUB_SHARED_MODE, timeout);
    }

    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
        oracleDbmsLockDao.convertLock(
                getLockHandle(getSchemaName(), lockNameType, lockName),
                OracleDbmsLockDao.EXCLUSIVE_MODE, timeout);
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
        oracleDbmsLockDao.releaseLock(
                getLockHandle(getSchemaName(), lockNameType, lockName));
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (OracleDbmsLockService.class) {
                    if (schemaName == null) {
                        schemaName = oracleDbmsLockDao.currentSchema();
                    }
                }
            }
            return schemaName;
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }
    }

    public String getLockHandle(String schemaName, String lockNameType, String lockName) {
        logger.debug("Requesting lock handle for lock with name '{}'.", lockName);
        String uniqueLockName =
                schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
        if (!lockIdentifiersCache) {
            return oracleDbmsLockDao.allocateLock(uniqueLockName, allocatedLockTimeToLiveInSecond);
        }
        DbmsLockInfo dbmsLockDto = dbmsLockHandle.get(uniqueLockName);
        if (dbmsLockDto == null || dbmsLockDto.getLockExpireTime().compareTo(new Date()) <= 0) {
            if (uniqueLockName.length() > DBMS_LOCK_NAME_MAX_LENGTH) {
                logger.error("Dbms lock name '{}' cannot be more than {} characters.",
                        uniqueLockName, DBMS_LOCK_NAME_MAX_LENGTH);
                throw new LockManagerRunTimeException("Dbms lock name cannot be more than " +
                        DBMS_LOCK_NAME_MAX_LENGTH + " characters.");
            }
            Calendar expireDate = Calendar.getInstance();
            expireDate.set(Calendar.SECOND, expireDate.get(Calendar.SECOND) + allocatedLockTimeToLiveInSecond);
            dbmsLockDto = new DbmsLockInfo(
                    uniqueLockName, oracleDbmsLockDao.allocateLock(uniqueLockName, allocatedLockTimeToLiveInSecond),
                    expireDate.getTime());
            dbmsLockHandle.put(uniqueLockName, dbmsLockDto);
        }
        return dbmsLockDto.getLockHandel();
    }
}
