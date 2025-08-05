package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import jakarta.persistence.*;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.procedure.ProcedureOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.tosan.tools.lockmanager.impl.dbms.dao.PostgresLockMode.SHARED_MODE;

/**
 * @author mortezaei
 * @since 11/19/2024
 */
public class PostgresqlDbmsLockDaoImpl implements DbmsLockDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlDbmsLockDaoImpl.class);
    public static final String CURRENT_SCHEMA_QUERY = "SELECT current_schema()";
    public static final String ALLOCATE_LOCK_QUERY = "dbms_lock.allocate_unique";
    public static final String RELEASE_LOCK_QUERY = "SELECT dbms_lock.release(:lockHandle)";
    private static final short DBMS_LOCK_NAME_MAX_LENGTH = 128;
    private static final int DEFAULT_READ_LOCK_TIMEOUT = 60;
    private static final int DEFAULT_WRIT_LOCK_TIMEOUT = 7200;
    private static final Map<String, DbmsLockInfo> dbmsLockHandle = new ConcurrentHashMap<>();
    @PersistenceContext
    private final EntityManager entityManager;
    private boolean lockIdentifiersCache = false;
    private int allocatedLockTimeToLiveInSecond = 864000;
    private String schemaName = null;

    public PostgresqlDbmsLockDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        this.lockIdentifiersCache = lockIdentifiersCache;
    }

    public void setAllocatedLockTimeToLiveInSecond(int allocatedLockTimeToLiveInSecond) {
        this.allocatedLockTimeToLiveInSecond = allocatedLockTimeToLiveInSecond;
    }

    @Override
    public String getSchemaName() {
        try {
            if (schemaName == null) {
                synchronized (PostgresqlDbmsLockDaoImpl.class) {
                    if (schemaName == null) {
                        schemaName = currentSchema(entityManager);
                    }
                }
            }
            return schemaName;
        } catch (Exception e) {
            throw new LockManagerRunTimeException(e.getMessage(), e);
        }
    }

    @Override
    public void requestLock(String lockNameType, String lockName, Integer timeout, boolean releaseOnCommit,
                            RequestLockType requestLockType) {
        if (requestLockType == null) {
            throw new LockManagerRunTimeException("request lock type is null.");
        }
        switch (requestLockType) {
            case READ -> requestLock(
                    entityManager,
                    getLockHandle(entityManager, getSchemaName(), lockNameType, lockName),
                    SHARED_MODE, timeout, releaseOnCommit);
            case WRITE -> requestLock(
                    entityManager,
                    getLockHandle(entityManager, getSchemaName(), lockNameType, lockName),
                    PostgresLockMode.EXCLUSIVE_MODE, timeout, releaseOnCommit);
            default -> throw new LockManagerRunTimeException("Invalid request lock type " + requestLockType);
        }
    }

    @Override
    public void convertLock(String lockNameType, String lockName, Integer timeout,
                            RequestLockType convertRequestLockType) {
        throw new LockManagerRunTimeException("convert not supported.");
    }

    @Override
    public void unLock(String lockNameType, String lockName) {
        releaseLock(
                entityManager,
                getLockHandle(entityManager, getSchemaName(), lockNameType, lockName));
    }

    public String getLockHandle(EntityManager entityManager, String schemaName, String lockNameType, String lockName) {
        LOGGER.debug("Requesting lock handle for lock with name '{}'.", lockName);
        String uniqueLockName =
                schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
        if (!lockIdentifiersCache) {
            return allocateLock(entityManager, uniqueLockName, allocatedLockTimeToLiveInSecond);
        }
        DbmsLockInfo dbmsLockDto = dbmsLockHandle.get(uniqueLockName);
        if (dbmsLockDto == null || dbmsLockDto.getLockExpireTime().compareTo(new Date()) <= 0) {
            if (uniqueLockName.length() > DBMS_LOCK_NAME_MAX_LENGTH) {
                LOGGER.error("Dbms lock name '{}' cannot be more than {} characters.",
                        uniqueLockName, DBMS_LOCK_NAME_MAX_LENGTH);
                throw new LockManagerRunTimeException("Dbms lock name cannot be more than " +
                        DBMS_LOCK_NAME_MAX_LENGTH + " characters.");
            }
            Calendar expireDate = Calendar.getInstance();
            expireDate.set(Calendar.SECOND, expireDate.get(Calendar.SECOND) + allocatedLockTimeToLiveInSecond);
            dbmsLockDto = new DbmsLockInfo(
                    uniqueLockName, allocateLock(entityManager, uniqueLockName, allocatedLockTimeToLiveInSecond),
                    expireDate.getTime());
            dbmsLockHandle.put(uniqueLockName, dbmsLockDto);
        }
        return dbmsLockDto.getLockHandel();
    }


    private String currentSchema(final EntityManager entityManager) {
        Query query = entityManager.createNativeQuery(CURRENT_SCHEMA_QUERY);
        return (String) query.getSingleResult();
    }

    private void requestLock(final EntityManager entityManager, final String lockHandle,
                             final PostgresLockMode lockMode, final Integer timeout, final boolean releaseOnCommit) {
        LOGGER.debug("Requesting lock with handle {}", lockHandle);
        final int[] callStatus = new int[1];
        //As DBMS_LOCK.REQUEST has boolean input param, cant use entityManager
        //                    .createNativeQuery because select does not support boolean value
        Session session = entityManager.unwrap(Session.class);
        try {
            session.doWork(connection -> {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("{ ? = call dbms_lock.request(?,?,?");
                if (releaseOnCommit) {
                    stringBuilder.append(",true");
                }
                stringBuilder.append(") }");
                try (CallableStatement call = connection.prepareCall(stringBuilder.toString())) {
                    call.registerOutParameter(1, Types.INTEGER);
                    call.setString(2, lockHandle);
                    call.setInt(3, lockMode.getValue());
                    call.setInt(4, timeout == null ? lockMode.equals(SHARED_MODE) ?
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
                throw new LockManagerRunTimeException("convert not supported.");
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

    private void releaseLock(EntityManager entityManager, final String lockHandle) {
        LOGGER.debug("Requesting release lock with handle {}", lockHandle);
        Query query = entityManager.createNativeQuery(RELEASE_LOCK_QUERY);
        query.setParameter("lockHandle", lockHandle);
        int result = ((Number) query.getSingleResult()).intValue();

        switch (result) {
            case 0, 4:
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
     * @param entityManager    entityManager
     * @param lockName         Name of the lock for which you want to generate a unique ID.
     * @param expirationSecond Length of time to leave lock allocated
     * @return The handle to the lock ID generated by ALLOCATE_UNIQUE.
     */
    private String allocateLock(final EntityManager entityManager, final String lockName, final int expirationSecond) {
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
            query.unwrap(ProcedureOutputs.class)
                    .release();
        }
    }
}
