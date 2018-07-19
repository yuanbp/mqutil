package com.metinform.common.loader;

import com.simple.datax.api.SimpleException;

public class InvalidSimpleObjectException
        extends SimpleException {
    private static final long serialVersionUID = 3105928222650505193L;
    private static final String MESSAGE = "Invalid Simple Object Type - ";

    public InvalidSimpleObjectException(String className) {
        super("Invalid Simple Object Type - " + className);
    }

    public InvalidSimpleObjectException(String className, Throwable cause) {
        super("Invalid Simple Object Type - " + className, cause);
    }
}



