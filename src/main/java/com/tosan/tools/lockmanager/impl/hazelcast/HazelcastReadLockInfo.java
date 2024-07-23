package com.tosan.tools.lockmanager.impl.hazelcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * @author R.Mehri
 * @since 29/09/2020
 */
public class HazelcastReadLockInfo implements Serializable {
    private String lockHandle;
    private List<String> members;
    private Date expireDate;

    public HazelcastReadLockInfo(String lockHandle, Date expireDate) {
        this.lockHandle = lockHandle;
        this.expireDate = expireDate;
    }

    public int getSize() {
        return members.size();
    }

    public void removeMembers(Collection<String> memberIds) {
        if (members == null) {
            members = new ArrayList<>();
        }
        for (String memberId : memberIds) {
            removeMember(memberId);
        }
    }

    public void removeMember(String memberId) {
        members.remove(memberId);
    }

    public void addMember(String memberId) {
        if (members == null) {
            members = new ArrayList<>();
        }
        members.add(memberId);
    }

    public void removeAllMembers() {
        if (members == null) {
            members = new ArrayList<>();
        }
        members.clear();
    }

    public String getLockHandle() {
        return lockHandle;
    }

    public void setLockHandle(String lockHandle) {
        this.lockHandle = lockHandle;
    }

    public boolean isExpire() {
        return expireDate.compareTo(new Date()) <= 0;
    }
}
