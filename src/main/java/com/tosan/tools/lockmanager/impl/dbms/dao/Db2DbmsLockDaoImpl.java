package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.invoker.Db2DbmsLockInvoker;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.apache.commons.lang3.StringUtils;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class Db2DbmsLockDaoImpl implements DbmsLockDao {
    private final Db2DbmsLockInvoker dbmsLockInvoker;
    private String schemaName = null;

    @PersistenceContext
    private final jakarta.persistence.EntityManager entityManager;

    public Db2DbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
        this.dbmsLockInvoker = new Db2DbmsLockInvoker();
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public Db2DbmsLockInvoker getDbmsLockInvoker() {
        return dbmsLockInvoker;
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (Db2DbmsLockDaoImpl.class) {
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

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        dbmsLockInvoker.requestLock(
                entityManager,
                getLockHandle(getSchemaName(), lockNameType, lockName),
                null, timeout, releaseOnCommit);
    }

    @Override
    public void requestReadLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
    }

    @Override
    public void convertToReadLock(String lockNameType, String lockName, Integer timeout) {
    }

    @Override
    public void convertToWriteLock(String lockNameType, String lockName, Integer timeout) {
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
    }

    public String getLockHandle(String schemaName, String lockNameType, String lockName) {
        return schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
    }
}
