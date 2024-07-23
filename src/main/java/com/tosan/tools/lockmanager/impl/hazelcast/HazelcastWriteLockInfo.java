package com.tosan.tools.lockmanager.impl.hazelcast;

import java.io.Serializable;
import java.util.Date;

/**
 * @author R.Mehri
 * @since 29/09/2020
 */
public class HazelcastWriteLockInfo implements Serializable {
    private String lockHandle;
    private String member;
    private Date expireDate;

    public HazelcastWriteLockInfo(String lockHandle, String members, Date expireDate) {
        this.lockHandle = lockHandle;
        this.member = members;
        this.expireDate = expireDate;
    }

    public String getLockHandle() {
        return lockHandle;
    }

    public void setLockHandle(String lockHandle) {
        this.lockHandle = lockHandle;
    }

    public void setMember(String members) {
        this.member = members;
    }

    public void setExpireDate(Date expireDate) {
        this.expireDate = expireDate;
    }

    public String getMember() {
        return member;
    }

    public Date getExpireDate() {
        return expireDate;
    }

    public boolean isExpire() {
        return expireDate.compareTo(new Date()) <= 0;
    }
}
