package com.tosan.tools.lockmanager.api;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;

/**
 * @author akhbari
 * @since 20/02/2019
 */
public interface LockManagementService {
    /**
     * Acquires the read lock if it is not held by another the given waiting time.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     *
     * @param lockNameType    lock name type
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the lock is held until it is explicitly released with
     *                        {@link #unlock(String)} or {@link #unlock(String, String)}  method or until the end of the transaction.
     * @throws LockManagerTimeoutException If the read lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void requestReadLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException;

    /**
     * Acquires the read lock if it is not held by another the given waiting time.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType    lock name type
     * @param lockName        lock name
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the lock is held until it is explicitly released with
     *                        {@link #unlock(String)} or {@link #unlock(String, String)}  method or until the end of the transaction.
     * @throws LockManagerTimeoutException If the read lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void requestReadLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException;

    /**
     * Acquires the read lock if it is not held by another the given waiting time.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType    lock name type
     * @param lockName        lock name
     * @param lockTimeout     Number of seconds to continue trying to grant the lock.
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the lock is held until it is explicitly released with
     *                        {@link #unlock(String)} or {@link #unlock(String, String)}  method or until the end of the transaction.
     * @throws LockManagerTimeoutException If the read lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void requestReadLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException;

    /**
     * Acquires the write lock if it is not held by another the given waiting time.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     *
     * @param lockNameType    lock name type
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the lock is held until it is explicitly released with
     *                        {@link #unlock(String)} or {@link #unlock(String, String)}  method or until the end of the transaction.
     * @throws LockManagerTimeoutException If the write lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void requestWriteLock(String lockNameType, boolean releaseOnCommit) throws LockManagerTimeoutException;

    /**
     * Acquires the write lock if it is not held by another the given waiting time.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType    lock name type
     * @param lockName        lock name
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the lock is held until it is explicitly released with
     *                        {@link #unlock(String)} or {@link #unlock(String, String)}  method or until the end of the transaction.
     * @throws LockManagerTimeoutException If the write lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void requestWriteLock(String lockNameType, String lockName, boolean releaseOnCommit) throws LockManagerTimeoutException;

    /**
     * Acquires the write lock if it is not held by another the given waiting time.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType    lock name type
     * @param lockName        lock name
     * @param lockTimeout     Number of seconds to continue trying to grant the lock.
     * @param releaseOnCommit If it is true, release the lock on commit or roll-back.
     *                        If it is false,the lock is held until it is explicitly released with
     *                        {@link #unlock(String)} or {@link #unlock(String, String)}  method or until the end of the transaction.
     * @throws LockManagerTimeoutException If the write lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void requestWriteLock(String lockNameType, String lockName, Integer lockTimeout, boolean releaseOnCommit) throws LockManagerTimeoutException;

    /**
     * This method explicitly releases a lock previously acquired.
     * Locks are automatically released at the end of a transaction.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     *
     * @param lockNameType نوع نام لاک
     * @throws LockManagerRunTimeException internal exception
     */
    void unlock(String lockNameType);

    /**
     * This method explicitly releases a lock previously acquired.
     * Locks are automatically released at the end of a transaction.
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @throws LockManagerRunTimeException internal exception
     */
    void unlock(String lockNameType, String lockName) throws LockManagerTimeoutException;

    /**
     * Converts a lock from one mode to another mode according to parameters
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @param lockTimeout  Number of seconds to continue trying to grant the lock.
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void convertToReadLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException;

    /**
     * Converts a lock from one mode to another mode according to parameters
     * طول نام lock دیتابیسی حداکثر ۱۲۸ کاراکتر است. برای یکتاسازی این مقدار نام schema به ابتدای نام lock افزوده می‌شود.
     * با توجه به اینکه طول نام schema در اوراکل حداکثر ۳۰ کاراکتر است،
     * مجموع طول lockName و lockNameType حداکثر ۹۶ کاراکتر می‌تواند باشد
     *
     * @param lockNameType نوع نام لاک
     * @param lockName     نام لاک
     * @param lockTimeout  Number of seconds to continue trying to grant the lock.
     * @throws LockManagerTimeoutException If the lock cannot be granted within this time period.
     * @throws LockManagerRunTimeException internal exception
     */
    void convertToWriteLock(String lockNameType, String lockName, Integer lockTimeout) throws LockManagerTimeoutException;
}
