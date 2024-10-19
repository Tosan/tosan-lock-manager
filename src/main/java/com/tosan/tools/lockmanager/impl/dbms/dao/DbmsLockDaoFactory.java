package com.tosan.tools.lockmanager.impl.dbms.dao;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;
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
public abstract class DbmsLockDaoFactory {

    public static DbmsLockDao getDbmsLockDao(EntityManager entityManager) {
        SessionFactoryImpl sessionFactory = entityManager.getEntityManagerFactory().unwrap(SessionFactoryImpl.class);
        Dialect dialect = sessionFactory.getJdbcServices().getDialect();
        if (dialect instanceof OracleDialect) {
            return new OracleDbmsLockDaoImpl(entityManager);
        } else if (dialect instanceof DB2Dialect) {
            return new Db2DbmsLockDaoImpl(entityManager);
        } else if (dialect instanceof PostgreSQLDialect) {
            return new PostgresqlDbmsLockDaoImpl(entityManager);
        }
        throw new LockManagerRunTimeException("the database dialect is not supported: " + dialect.getClass());
    }
}
