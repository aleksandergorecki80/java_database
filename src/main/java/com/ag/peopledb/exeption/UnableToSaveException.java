package com.ag.peopledb.exeption;

public class UnableToSaveException extends RuntimeException {
    public UnableToSaveException(String message) {
        super(message);
    }
}
