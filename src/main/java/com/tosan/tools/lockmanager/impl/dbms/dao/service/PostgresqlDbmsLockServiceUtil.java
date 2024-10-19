package com.tosan.tools.lockmanager.impl.dbms.dao.service;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.exception.LockManagerTimeoutException;
import com.tosan.tools.lockmanager.impl.dbms.dao.exception.DaoRuntimeException;
import com.tosan.tools.lockmanager.impl.dbms.dao.util.DbmsLockInfo;
import jakarta.persistence.EntityManager;
import jakarta.persistence.ParameterMode;
import jakarta.persistence.StoredProcedureQuery;
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

/**
 * @author mortezaei
 * @since 11/19/2024
 *
 * This util uses pg_dbms_lock package to handle locks.
 */
public class PostgresqlDbmsLockServiceUtil {
    public static final Integer SHARED_MODE = 4;
    public static final Integer EXCLUSIVE_MODE = 6;
    public static final String GET_SCHEMA_NAME_QUERY = "SELECT current_schema()";
    public static final String RELEASE_LOCK_QUERY = "SELECT dbms_lock.release(:lockHandle)";
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlDbmsLockServiceUtil.class);
    private static final int DEFAULT_READ_LOCK_TIMEOUT = 60;
    private static final int DEFAULT_WRIT_LOCK_TIMEOUT = 7200;
    private static final Map<String, DbmsLockInfo> dbmsLockHandle = new ConcurrentHashMap<>();
    private boolean lockIdentifiersCache = false;
    private int allocatedLockTimeToLiveInSecond = 864000;

    public void setLockIdentifiersCache(boolean lockIdentifiersCache) {
        this.lockIdentifiersCache = lockIdentifiersCache;
    }

    public void setAllocatedLockTimeToLiveInSecond(Integer allocatedLockTimeToLiveInSecond) {
        this.allocatedLockTimeToLiveInSecond = allocatedLockTimeToLiveInSecond;
    }

    public void requestLock(final EntityManager entityManager, final String lockHandle, final Integer lockMode,
                            final Integer timeout, final boolean releaseOnCommit) {
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
                    call.setInt(3, lockMode);
                    call.setInt(4, timeout == null ? lockMode.equals(SHARED_MODE) ?
                            DEFAULT_READ_LOCK_TIMEOUT : DEFAULT_WRIT_LOCK_TIMEOUT : timeout);
                    call.execute();
                    callStatus[0] = call.getInt(1);
                }
            });
        } catch (Throwable e) {
            throw new DaoRuntimeException(e.getMessage());
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

    public String getLockHandle(EntityManager entityManager, String schemaName, String lockNameType, String lockName) {
        LOGGER.debug("Requesting lock handle for lock with name '{}'.", lockName);
        String uniqueLockName =
                schemaName + "-" + lockNameType + (StringUtils.isNotEmpty(lockName) ? "-" + lockName : "");
        if (!lockIdentifiersCache) {
            return getLockHandle(entityManager, uniqueLockName, allocatedLockTimeToLiveInSecond);
        }
        DbmsLockInfo dbmsLockDto = dbmsLockHandle.get(uniqueLockName);
        if (dbmsLockDto == null || dbmsLockDto.getLockExpireTime().compareTo(new Date()) <= 0) {
            short DBMS_LOCK_NAME_MAX_LENGTH = 128;
            if (uniqueLockName.length() > DBMS_LOCK_NAME_MAX_LENGTH) {
                LOGGER.error("Dbms lock name '{}' cannot be more than {} characters.",
                        uniqueLockName, DBMS_LOCK_NAME_MAX_LENGTH);
                throw new LockManagerRunTimeException("Dbms lock name cannot be more than " +
                        DBMS_LOCK_NAME_MAX_LENGTH + " characters.");
            }
            Calendar expireDate = Calendar.getInstance();
            expireDate.set(Calendar.SECOND, expireDate.get(Calendar.SECOND) + allocatedLockTimeToLiveInSecond);
            dbmsLockDto = new DbmsLockInfo(
                    uniqueLockName, getLockHandle(entityManager, uniqueLockName, allocatedLockTimeToLiveInSecond),
                    expireDate.getTime());
            dbmsLockHandle.put(uniqueLockName, dbmsLockDto);
        }
        return dbmsLockDto.getLockHandel();
    }

    /**
     * 'getLockHandle' using ALLOCATE_UNIQUE procedure in DBMS_LOCK package that allocates a unique lock identifier
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
    public String getLockHandle(final EntityManager entityManager, final String lockName, final int expirationSecond) {
        StoredProcedureQuery query = entityManager
                .createStoredProcedureQuery("dbms_lock.allocate_unique")
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
