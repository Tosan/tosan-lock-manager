package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.impl.dbms.dao.exception.DaoRuntimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.service.Db2DbmsLockServiceUtil;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class Db2DbmsLockDaoImpl implements DbmsLockDao {
    private String schemaName = null;
    private final Db2DbmsLockServiceUtil db2DbmsLockServiceUtil;

    @PersistenceContext
    private jakarta.persistence.EntityManager entityManager;

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public Db2DbmsLockServiceUtil getDb2DbmsLockServiceUtil() {
        return db2DbmsLockServiceUtil;
    }

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        db2DbmsLockServiceUtil.setLockIdentifiersCache(lockIdentifiersCache);
    }

    public Db2DbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
        db2DbmsLockServiceUtil = new Db2DbmsLockServiceUtil();
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (Db2DbmsLockDaoImpl.class) {
                    if (schemaName == null) {
                        Query query = getEntityManager().createNativeQuery(Db2DbmsLockServiceUtil.GET_SCHEMA_NAME_QUERY);
                        schemaName = (String) query.getSingleResult();
                    }
                }
            }
            return schemaName;
        } catch (Throwable e) {
            throw new DaoRuntimeException(e.getMessage());
        }
    }

    @Override
    public void requestWriteLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit) {
        db2DbmsLockServiceUtil.requestWriteLock(entityManager, getSchemaName(),
                db2DbmsLockServiceUtil.getLockHandle(getSchemaName(), lockNameType, lockName));
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
}
