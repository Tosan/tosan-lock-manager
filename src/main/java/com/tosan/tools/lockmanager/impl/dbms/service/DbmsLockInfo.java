package com.tosan.tools.lockmanager.impl.dbms.service;

import java.util.Date;

/**
 * @author akhbari
 * @since 02/03/2019
 */
public class DbmsLockInfo {
    private String lockName;
    private String lockHandel;
    private Date lockExpireTime;

    public DbmsLockInfo(String lockName, String lockHandel, Date lockExpireTime) {
        this.lockName = lockName;
        this.lockHandel = lockHandel;
        this.lockExpireTime = lockExpireTime;
    }

    public String getLockName() {
        return lockName;
    }

    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    public String getLockHandel() {
        return lockHandel;
    }

    public void setLockHandel(String lockHandel) {
        this.lockHandel = lockHandel;
    }

    public Date getLockExpireTime() {
        return lockExpireTime;
    }

    public void setLockExpireTime(Date lockExpireTime) {
        this.lockExpireTime = lockExpireTime;
    }
}
