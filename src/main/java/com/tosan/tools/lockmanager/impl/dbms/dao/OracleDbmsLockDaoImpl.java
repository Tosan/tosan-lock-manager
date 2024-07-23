package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.exception.DaoRuntimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.service.OracleDbmsLockServiceUtil;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class OracleDbmsLockDaoImpl implements DbmsLockDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDbmsLockDaoImpl.class);
    private String schemaName = null;
    private final OracleDbmsLockServiceUtil oracleDbmsLockServiceUtil;

    public OracleDbmsLockServiceUtil getOracleDbmsLockServiceUtil() {
        return oracleDbmsLockServiceUtil;
    }

    @PersistenceContext
    private jakarta.persistence.EntityManager entityManager;

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public OracleDbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
        oracleDbmsLockServiceUtil = new OracleDbmsLockServiceUtil();
    }

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        oracleDbmsLockServiceUtil.setLockIdentifiersCache(lockIdentifiersCache);
    }

    public void setAllocatedLockTimeToLiveInSecond(int allocatedLockTimeToLiveInSecond) {
        oracleDbmsLockServiceUtil.setAllocatedLockTimeToLiveInSecond(allocatedLockTimeToLiveInSecond);
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        oracleDbmsLockServiceUtil.requestLock(entityManager, oracleDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(),
                        lockNameType, lockName),
                OracleDbmsLockServiceUtil.SUB_SHARED_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        oracleDbmsLockServiceUtil.requestLock(entityManager, oracleDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(),
                        lockNameType, lockName),
                OracleDbmsLockServiceUtil.EXCLUSIVE_MODE, timeout, releaseOnCommit);
    }

    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
        oracleDbmsLockServiceUtil.convertLock(entityManager, oracleDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(),
                        lockNameType, lockName),
                OracleDbmsLockServiceUtil.SUB_SHARED_MODE, timeout);
    }

    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
        oracleDbmsLockServiceUtil.convertLock(entityManager, oracleDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(),
                        lockNameType, lockName),
                OracleDbmsLockServiceUtil.EXCLUSIVE_MODE, timeout);
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
        releaseLock(oracleDbmsLockServiceUtil.getLockHandle(entityManager, getSchemaName(), lockNameType, lockName));
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (OracleDbmsLockDaoImpl.class) {
                    if (schemaName == null) {
                        Query query = getEntityManager().createNativeQuery(OracleDbmsLockServiceUtil.GET_SCHEMA_NAME_QUERY);
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
        Query query = getEntityManager().createNativeQuery(OracleDbmsLockServiceUtil.RELEASE_LOCK_QUERY);
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
