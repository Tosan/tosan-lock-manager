package com.tosan.tools.lockmanager.impl.dbms.dao.service;

import jakarta.persistence.EntityManager;
import com.tosan.tools.lockmanager.impl.dbms.dao.exception.DaoRuntimeException;
import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author akhbari
 * @since 10/03/2019
 */
public class Db2DbmsLockServiceUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2DbmsLockServiceUtil.class);
    public static final String GET_SCHEMA_NAME_QUERY = "SELECT current_schema FROM sysibm.sysdummy1";

    private boolean lockIdentifiersCache = false;

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        this.lockIdentifiersCache = lockIdentifiersCache;
    }

    public void requestWriteLock(final EntityManager entityManager, final String schemaName, final String lockName) {
        LOGGER.debug("Requesting write lock with lock name {}", lockName);
        final Number callStatus ;
        try {
            callStatus = (Number) entityManager
                    .createNativeQuery(
                            "SELECT" + schemaName + ".REQUEST_WRITE_LOCK(:lockName)"
                    )
                    .setParameter("lockName", lockName)
                    .getSingleResult();
        } catch (Throwable e) {
            throw new DaoRuntimeException(e.getMessage());
        }
        switch (callStatus.intValue()) {
            case 0:
                LOGGER.debug("Acquired write lock with lock name {}", lockName);
                return;
            case 1:
                throw new LockManagerTimeoutException("Timeout error occurred in 'REQUEST_WRITE_LOCK' procedure.");
            default:
                throw new LockManagerRunTimeException("Error occurred in 'REQUEST_WRITE_LOCK' procedure.");
        }
    }

    public String getLockHandle(String schemaName, String lockNameType, String lockName) {
        return schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
    }
}
