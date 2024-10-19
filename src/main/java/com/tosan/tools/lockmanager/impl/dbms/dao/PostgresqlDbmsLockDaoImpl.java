package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.exception.DaoRuntimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.service.PostgresqlDbmsLockServiceUtil;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mortezaei
 * @since 11/19/2024
 */
public class PostgresqlDbmsLockDaoImpl implements DbmsLockDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlDbmsLockDaoImpl.class);
    private String schemaName = null;
    private final PostgresqlDbmsLockServiceUtil postgresqlDbmsLockServiceUtil;

    public PostgresqlDbmsLockServiceUtil getPostgresqlDbmsLockServiceUtil() {
        return postgresqlDbmsLockServiceUtil;
    }

    @PersistenceContext
    private EntityManager entityManager;

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public PostgresqlDbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
        postgresqlDbmsLockServiceUtil = new PostgresqlDbmsLockServiceUtil();
    }

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        postgresqlDbmsLockServiceUtil.setLockIdentifiersCache(lockIdentifiersCache);
    }

    public void setAllocatedLockTimeToLiveInSecond(int allocatedLockTimeToLiveInSecond) {
        postgresqlDbmsLockServiceUtil.setAllocatedLockTimeToLiveInSecond(allocatedLockTimeToLiveInSecond);
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        postgresqlDbmsLockServiceUtil.requestLock(entityManager, postgresqlDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(),
                        lockNameType, lockName),
                PostgresqlDbmsLockServiceUtil.SHARED_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        postgresqlDbmsLockServiceUtil.requestLock(entityManager, postgresqlDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(),
                        lockNameType, lockName),
                PostgresqlDbmsLockServiceUtil.EXCLUSIVE_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
    }

    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
        releaseLock(postgresqlDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(), lockNameType, lockName));
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (PostgresqlDbmsLockDaoImpl.class) {
                    if (schemaName == null) {
                        Query query = getEntityManager().createNativeQuery(PostgresqlDbmsLockServiceUtil.GET_SCHEMA_NAME_QUERY);
                        schemaName = (String) query.getSingleResult();
                    }
                }
            }
            return schemaName;
        } catch (Throwable e) {
            throw new DaoRuntimeException(e.getMessage());
        }
    }

    private void releaseLock(final String lockHandle) {
        LOGGER.debug("Requesting release lock with handle {}", lockHandle);
        Query query = getEntityManager().createNativeQuery(PostgresqlDbmsLockServiceUtil.RELEASE_LOCK_QUERY);
        query.setParameter("lockHandle", lockHandle);
        int result = ((Number) query.getSingleResult()).intValue();

        switch (result) {
            case 0:
            case 4:
                LOGGER.debug("Released lock with handle {}", lockHandle);
                return;
            case 3:
                throw new LockManagerRunTimeException("Parameter error occurred in 'DBMS_LOCK' release.");
            case 5:
                throw new LockManagerRunTimeException("Illegal lock handle error occurred in 'DBMS_LOCK' release.");
            default:
                throw new LockManagerRunTimeException("Error occurred in 'DBMS_LOCK' release.");
        }
    }
}
