package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;

/**
 * @author akhbari
 * @since 23/02/2019
 */
public interface DbmsLockDao {

    /**
     * This method returns schema name. Schema name is used in lock requests to make them unique in DBMS level.
     *
     * @return the current schema name
     */
    String getSchemaName();

    /**
     * Acquires the write lock if it is not held by another the given waiting time.
     *
     * @param lockNameType    lock name type
     * @param lockName        lock name
     * @param timeout         Number of seconds to continue trying to grant the write requestReadLock.
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the write is held until it is explicitly released with
     *                        {@link #unLock(String, String)} method or until the end of the session.
     * @param requestLockType request lock type
     * @throws LockManagerTimeoutException If the write lock cannot be granted within this time period.
     */
    void requestLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit,
                     RequestLockType requestLockType);

    /**
     * Converts a lock from one mode to write mode.
     *
     * @param lockNameType           lock name type
     * @param lockName               lock name
     * @param timeout                Number of seconds to continue trying to grant the write lock.
     * @param convertRequestLockType Converts a lock from one mode to this type.
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     */
    void convertLock(String lockNameType, String lockName, Integer timeout, RequestLockType convertRequestLockType);

    /**
     * This method explicitly releases a lock previously acquired.
     * Locks are automatically released at the end of a session.
     *
     * @param lockNameType lock name type
     * @param lockName     lock name
     */
    void unLock(String lockNameType, String lockName);
}
