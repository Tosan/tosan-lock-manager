package com.tosan.tools.lockmanager.impl.dbms.dao.invoker;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author akhbari
 * @since 10/03/2019
 */
public class Db2DbmsLockInvoker implements DbmsLockInvoker {
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2DbmsLockInvoker.class);
    public static final String GET_SCHEMA_NAME_QUERY = "SELECT current_schema FROM sysibm.sysdummy1";

    @Override
    public String currentSchema(final EntityManager entityManager) {
        Query query = entityManager.createNativeQuery(GET_SCHEMA_NAME_QUERY);
        return (String) query.getSingleResult();
    }

    @Override
    public String allocateLock(EntityManager entityManager, String lockName, int expirationSecond) {
        return null;
    }

    @Override
    public void requestLock(EntityManager entityManager, String lockHandle, Integer lockMode, Integer timeout, boolean releaseOnCommit) {
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

    @Override
    public void convertLock(EntityManager entityManager, String lockHandle, Integer lockMode, Integer timeout) {
    }

    @Override
    public void releaseLock(EntityManager entityManager, String lockHandle) {
    }
}
