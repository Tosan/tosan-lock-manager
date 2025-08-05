package com.tosan.tools.lockmanager.impl.dbms.service;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.Db2DbmsLockDao;
import jakarta.persistence.EntityManager;
import org.apache.commons.lang3.StringUtils;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class Db2DbmsLockService implements DbmsLockService {
    private final Db2DbmsLockDao db2DbmsLockDao;
    private String schemaName = null;

    public Db2DbmsLockService(EntityManager entityManager) {
        this.db2DbmsLockDao = new Db2DbmsLockDao(entityManager);
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (Db2DbmsLockService.class) {
                    if (schemaName == null) {
                        schemaName = db2DbmsLockDao.currentSchema();
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
        db2DbmsLockDao.requestLock(
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
