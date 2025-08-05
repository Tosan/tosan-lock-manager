package com.tosan.tools.lockmanager.impl.dbms.dao;

/**
 * @author Ali Alimohammadi
 * @since 8/4/25
 */
public enum OracleLockMode {
    SUB_SHARED_MODE(2),
    EXCLUSIVE_MODE(6);

    private final Integer value;

    OracleLockMode(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
