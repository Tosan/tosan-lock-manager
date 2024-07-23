package com.tosan.tools.lockmanager.impl.dbms.dao.exception;

import com.tosan.tools.lockmanager.exception.LockManagerRunTimeException;

/**
 * @author akhbari
 * @since 24/02/2019
 */
public class DaoRuntimeException extends LockManagerRunTimeException {

    public DaoRuntimeException(String message) {
        super(message);
    }

    public DaoRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
