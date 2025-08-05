package com.tosan.tools.lockmanager.impl.dbms.dao;

/**
 * @author akhbari
 * @since 23/02/2019
 */
public interface DbmsLockDao {

    String currentSchema();

    String allocateLock(final String lockName, final int expirationSecond);

    void requestLock(final String lockHandle, final Integer lockMode,
                     final Integer timeout, final boolean releaseOnCommit);

    void convertLock(final String lockHandle, final Integer lockMode,
                     final Integer timeout);

    void releaseLock(final String lockHandle);
}
