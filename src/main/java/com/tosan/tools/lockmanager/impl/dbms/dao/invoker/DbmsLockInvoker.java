package com.tosan.tools.lockmanager.impl.dbms.dao.invoker;

import jakarta.persistence.EntityManager;

/**
 * @author MosiDev
 * @since 8/4/25
 */
public interface DbmsLockInvoker {

    String currentSchema(final EntityManager entityManager);

    String allocateLock(final EntityManager entityManager, final String lockName, final int expirationSecond);

    void requestLock(final EntityManager entityManager, final String lockHandle, final Integer lockMode,
                     final Integer timeout, final boolean releaseOnCommit);

    void convertLock(final EntityManager entityManager, final String lockHandle, final Integer lockMode,
                     final Integer timeout);

    void releaseLock(final EntityManager entityManager, final String lockHandle);
}
