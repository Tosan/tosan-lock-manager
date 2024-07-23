package com.tosan.tools.lockmanager.exception;

/**
 * @author akhbari
 * @since 27/02/2019
 */
public class LockManagerRunTimeException extends RuntimeException {

    public LockManagerRunTimeException(String message) {
        super(message);
    }

    public LockManagerRunTimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
