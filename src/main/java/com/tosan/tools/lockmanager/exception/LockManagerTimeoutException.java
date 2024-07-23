package com.tosan.tools.lockmanager.exception;

/**
 * @author akhbari
 * @since 03/03/2019
 */
public class LockManagerTimeoutException extends LockManagerRunTimeException {

    public LockManagerTimeoutException(String message) {
        super(message);
    }

    public LockManagerTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
