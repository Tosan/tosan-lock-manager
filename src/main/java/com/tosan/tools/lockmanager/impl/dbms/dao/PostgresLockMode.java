package com.tosan.tools.lockmanager.impl.dbms.dao;

/**
 * @author Ali Alimohammadi
 * @since 8/4/25
 */
public enum PostgresLockMode {
    SHARED_MODE(4),
    EXCLUSIVE_MODE(6);

    private final Integer value;

    PostgresLockMode(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
