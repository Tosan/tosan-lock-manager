package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
import com.tosan.tools.lockmanager.impl.dbms.service.Db2DbmsLockService;
import com.tosan.tools.lockmanager.impl.dbms.service.DbmsLockService;
import com.tosan.tools.lockmanager.impl.dbms.service.OracleDbmsLockService;
import com.tosan.tools.lockmanager.impl.dbms.service.PostgresqlDbmsLockService;
import jakarta.persistence.EntityManager;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.internal.SessionFactoryImpl;

/**
 * @author akhbari
 * @since 03/03/2019
 */
public abstract class DbmsLockServiceFactory {

    public static DbmsLockService getDbmsLockService(EntityManager entityManager) {
        SessionFactoryImpl sessionFactory = entityManager.getEntityManagerFactory().unwrap(SessionFactoryImpl.class);
        Dialect dialect = sessionFactory.getJdbcServices().getDialect();
        if (dialect instanceof OracleDialect) {
            return new OracleDbmsLockService(entityManager);
        } else if (dialect instanceof DB2Dialect) {
            return new Db2DbmsLockService(entityManager);
        } else if (dialect instanceof PostgreSQLDialect) {
            return new PostgresqlDbmsLockService(entityManager);
        }
        throw new LockManagerRunTimeException("the database dialect is not supported: " + dialect.getClass());
    }
}
