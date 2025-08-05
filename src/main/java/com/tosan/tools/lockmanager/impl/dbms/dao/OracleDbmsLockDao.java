package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import jakarta.persistence.*;
import org.hibernate.Session;
import org.hibernate.procedure.ProcedureOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.CallableStatement;
import java.sql.Types;

/**
 * @author akhbari
 * @since 04/03/2019
 */
public class OracleDbmsLockDao implements DbmsLockDao {
    public static final String CURRENT_SCHEMA_QUERY = "SELECT SYS_CONTEXT('userenv','CURRENT_SCHEMA') FROM DUAL";
    public static final String ALLOCATE_LOCK_QUERY = "DBMS_LOCK.ALLOCATE_UNIQUE";
    public static final String RELEASE_LOCK_QUERY = "SELECT DBMS_LOCK.RELEASE(:lockHandle) FROM DUAL";
    public static final String CONVERT_LOCK_QUERY = "SELECT DBMS_LOCK.CONVERT(:lockHandle, :lockMode, :timeout) FROM DUAL";
    public static final Integer SUB_SHARED_MODE = 2;
    public static final Integer EXCLUSIVE_MODE = 6;
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDbmsLockDao.class);
    private static final int DEFAULT_READ_LOCK_TIMEOUT = 60;
    private static final int DEFAULT_WRIT_LOCK_TIMEOUT = 7200;
    @PersistenceContext
    private final jakarta.persistence.EntityManager entityManager;

    public OracleDbmsLockDao(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public String currentSchema() {
        Query query = entityManager.createNativeQuery(CURRENT_SCHEMA_QUERY);
        return (String) query.getSingleResult();
    }

    @Override
    public void requestLock(final String lockHandle, final Integer lockMode,
                            final Integer timeout, final boolean releaseOnCommit) {
        LOGGER.debug("Requesting lock with handle {}", lockHandle);
        final int[] callStatus = new int[1];
        //As DBMS_LOCK.REQUEST has boolean input param,
        //can't use entityManager.createNativeQuery because select does not support boolean value
        Session session = entityManager.unwrap(Session.class);
        try {
            session.doWork(connection -> {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("{ ? = call DBMS_LOCK.REQUEST(?,?,?");
                if (releaseOnCommit) {
                    stringBuilder.append(",TRUE");
                }
                stringBuilder.append(") }");
                try (CallableStatement call = connection.prepareCall(stringBuilder.toString())) {
                    call.registerOutParameter(1, Types.INTEGER);
                    call.setString(2, lockHandle);
                    call.setInt(3, lockMode);
                    call.setInt(4, timeout == null ? lockMode.equals(SUB_SHARED_MODE) ?
                            DEFAULT_READ_LOCK_TIMEOUT : DEFAULT_WRIT_LOCK_TIMEOUT : timeout);
                    call.execute();
                    callStatus[0] = call.getInt(1);
                }
            });
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }
        switch (callStatus[0]) {
            case 0:
                LOGGER.debug("Acquired lock with handle {}.", lockHandle);
                return;
            case 4:
                convertLock(lockHandle, lockMode, timeout);
                LOGGER.debug("Convert lock. lock mode:{} ", lockMode);
                return;
            case 1:
                throw new LockManagerTimeoutException("Timeout error occurred in 'DBMS_LOCK' request.");
            case 2:
                throw new LockManagerRunTimeException("deadlock error occurred in 'DBMS_LOCK' request.");
            case 3:
                throw new LockManagerRunTimeException("Parameter error occurred in 'DBMS_LOCK' request.");
            case 5:
                throw new LockManagerRunTimeException("Illegal lock handle error occurred in 'DBMS_LOCK' request.");
            default:
                throw new LockManagerRunTimeException("Error occurred in 'DBMS_LOCK' request.");
        }
    }

    @Override
    public void convertLock(final String lockHandle, final Integer lockMode, final Integer timeout) {
        LOGGER.debug("Requesting convert lock with handle {}", lockHandle);
        final Number callStatus;
        try {
            callStatus = (Number) entityManager
                    .createNativeQuery(CONVERT_LOCK_QUERY)
                    .setParameter("lockHandle", lockHandle)
                    .setParameter("lockMode", lockMode)
                    .setParameter("timeout", timeout == null ? lockMode.equals(SUB_SHARED_MODE) ?
                            DEFAULT_READ_LOCK_TIMEOUT : DEFAULT_WRIT_LOCK_TIMEOUT : timeout)
                    .getSingleResult();
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }

        switch (callStatus.intValue()) {
            case 0:
                LOGGER.debug("Converted lock with handle {}", lockHandle);
                return;
            case 1:
                throw new LockManagerTimeoutException("Timeout error occurred in 'DBMS_LOCK' convert.");
            case 2:
                throw new LockManagerRunTimeException("deadlock error occurred in 'DBMS_LOCK' convert.");
            case 3:
                throw new LockManagerRunTimeException("Parameter error occurred in 'DBMS_LOCK' convert.");
            case 4:
                throw new LockManagerRunTimeException("Don't own lock specified by lock handle occurred in 'DBMS_LOCK' convert.");
            case 5:
                throw new LockManagerRunTimeException("Illegal lock handle error occurred in 'DBMS_LOCK' convert.");
            default:
                throw new LockManagerRunTimeException("Error occurred in 'DBMS_LOCK' convert.");
        }
    }

    @Override
    public void releaseLock(final String lockHandle) {
        LOGGER.debug("Requesting release lock with handle {}", lockHandle);
        Query query = entityManager.createNativeQuery(RELEASE_LOCK_QUERY);
        query.setParameter("lockHandle", lockHandle);
        int result = ((Number) query.getSingleResult()).intValue();

        switch (result) {
            case 0:
            case 4:
                LOGGER.debug("Released lock with handle {}", lockHandle);
                return;
            case 3:
                throw new LockManagerRunTimeException("Parameter error occurred in 'DBMS_LOCK' release.");
            case 5:
                throw new LockManagerRunTimeException("Illegal lock handle error occurred in 'DBMS_LOCK' release.");
            default:
                throw new LockManagerRunTimeException("Error occurred in 'DBMS_LOCK' release.");
        }
    }

    /**
     * 'allocateLock' using ALLOCATE_UNIQUE procedure in DBMS_LOCK package that allocates a unique lock identifier
     * (in the range of 1073741824 to 1999999999) given a lock name.
     * Lock identifiers are used to enable applications to coordinate their use of locks.
     * A lock name is associated with the returned lock ID for at least expiration_secs (defaults to 10 days) past the
     * last call to 'getLockHandle' with the given lock name.
     * After this time, the row in the dbms_lock_allocated table for this lock name may be deleted in order to recover space.
     *
     * @param lockName         Name of the lock for which you want to generate a unique ID.
     * @param expirationSecond Length of time to leave lock allocated
     * @return The handle to the lock ID generated by ALLOCATE_UNIQUE.
     */
    @Override
    public String allocateLock(final String lockName, final int expirationSecond) {
        StoredProcedureQuery query = entityManager
                .createStoredProcedureQuery(ALLOCATE_LOCK_QUERY)
                .registerStoredProcedureParameter(1, String.class, ParameterMode.IN)
                .registerStoredProcedureParameter(2, String.class, ParameterMode.OUT)
                .registerStoredProcedureParameter(3, Integer.class, ParameterMode.IN)
                .setParameter(1, lockName)
                .setParameter(3, expirationSecond);
        try {
            query.execute();
            String lockHandle = (String) query.getOutputParameterValue(2);
            LOGGER.debug("Acquired lock handle {} for lock with name '{}'.", lockHandle, lockName);
            return lockHandle;
        } finally {
            query.unwrap(ProcedureOutputs.class).release();
        }
    }
}
