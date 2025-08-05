package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class Db2DbmsLockDaoImpl implements DbmsLockDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2DbmsLockDaoImpl.class);
    public static final String GET_SCHEMA_NAME_QUERY = "SELECT current_schema FROM sysibm.sysdummy1";
    @PersistenceContext
    private final jakarta.persistence.EntityManager entityManager;
    private String schemaName = null;

    public Db2DbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (Db2DbmsLockDaoImpl.class) {
                    if (schemaName == null) {
                        schemaName = currentSchema(entityManager);
                    }
                }
            }
            return schemaName;
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }
    }

    @Override
    public void requestLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit,
                            RequestLockType requestLockType) {
        if (requestLockType == null) {
            throw new LockManagerRunTimeException("request lock type is null.");
        }
        if (requestLockType == RequestLockType.WRITE) {
            requestLock(
                    entityManager,
                    getLockHandle(getSchemaName(), lockNameType, lockName),
                    null, timeout, releaseOnCommit);
        } else {
            throw new LockManagerRunTimeException("Invalid request lock type " + requestLockType);
        }

    }

    @Override
    public void convertLock(String lockNameType, String lockName, Integer timeout,
                            RequestLockType convertRequestLockType) {
        throw new LockManagerRunTimeException("convert not supported.");
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
        throw new LockManagerRunTimeException("release not supported.");
    }

    public String getLockHandle(String schemaName, String lockNameType, String lockName) {
        return schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
    }

    private String currentSchema(final EntityManager entityManager) {
        Query query = entityManager.createNativeQuery(GET_SCHEMA_NAME_QUERY);
        return (String) query.getSingleResult();
    }

    private void requestLock(EntityManager entityManager, String lockHandle, Integer lockMode, Integer timeout,
                             boolean releaseOnCommit) {
        LOGGER.debug("Requesting write lock with lock name {}", lockHandle);
        final Number callStatus;
        try {
            callStatus = (Number) entityManager
                    .createNativeQuery("SELECT " + currentSchema(entityManager) + ".REQUEST_WRITE_LOCK(:lockName)")
                    .setParameter("lockName", lockHandle)
                    .getSingleResult();
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }
        switch (callStatus.intValue()) {
            case 0:
                LOGGER.debug("Acquired write lock with lock name {}", lockHandle);
                return;
            case 1:
                throw new LockManagerTimeoutException("Timeout error occurred in 'REQUEST_WRITE_LOCK' procedure.");
            default:
                throw new LockManagerRunTimeException("Error occurred in 'REQUEST_WRITE_LOCK' procedure.");
        }
    }
}
