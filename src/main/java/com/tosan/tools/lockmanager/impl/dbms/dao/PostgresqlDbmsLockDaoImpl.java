package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.invoker.PostgresqlDbmsLockInvoker;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mortezaei
 * @since 11/19/2024
 */
public class PostgresqlDbmsLockDaoImpl implements DbmsLockDao {
    private static final Logger logger = LoggerFactory.getLogger(PostgresqlDbmsLockDaoImpl.class);
    private static final short DBMS_LOCK_NAME_MAX_LENGTH = 128;
    private static final Map<String, DbmsLockInfo> dbmsLockHandle = new ConcurrentHashMap<>();
    private final PostgresqlDbmsLockInvoker dbmsLockInvoker;
    private boolean lockIdentifiersCache = false;
    private int allocatedLockTimeToLiveInSecond = 864000;
    private String schemaName = null;

    @PersistenceContext
    private final EntityManager entityManager;

    public PostgresqlDbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
        this.dbmsLockInvoker = new PostgresqlDbmsLockInvoker();
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public PostgresqlDbmsLockInvoker getDbmsLockInvoker() {
        return dbmsLockInvoker;
    }

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        this.lockIdentifiersCache = lockIdentifiersCache;
    }

    public void setAllocatedLockTimeToLiveInSecond(int allocatedLockTimeToLiveInSecond) {
        this.allocatedLockTimeToLiveInSecond = allocatedLockTimeToLiveInSecond;
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        dbmsLockInvoker.requestLock(
                entityManager,
                getLockHandle(entityManager, getSchemaName(), lockNameType, lockName),
                PostgresqlDbmsLockInvoker.SHARED_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        dbmsLockInvoker.requestLock(
                entityManager,
                getLockHandle(entityManager, getSchemaName(), lockNameType, lockName),
                PostgresqlDbmsLockInvoker.EXCLUSIVE_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
    }

    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
        dbmsLockInvoker.releaseLock(
                entityManager,
                getLockHandle(entityManager, getSchemaName(), lockNameType, lockName));
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (PostgresqlDbmsLockDaoImpl.class) {
                    if (schemaName == null) {
                        schemaName = dbmsLockInvoker.currentSchema(entityManager);
                    }
                }
            }
            return schemaName;
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }
    }

    public String getLockHandle(EntityManager entityManager, String schemaName, String lockNameType, String lockName) {
        logger.debug("Requesting lock handle for lock with name '{}'.", lockName);
        String uniqueLockName =
                schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
        if (!lockIdentifiersCache) {
            return dbmsLockInvoker.allocateLock(entityManager, uniqueLockName, allocatedLockTimeToLiveInSecond);
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
                    uniqueLockName, dbmsLockInvoker.allocateLock(entityManager, uniqueLockName, allocatedLockTimeToLiveInSecond),
                    expireDate.getTime());
            dbmsLockHandle.put(uniqueLockName, dbmsLockDto);
        }
        return dbmsLockDto.getLockHandel();
    }
}
