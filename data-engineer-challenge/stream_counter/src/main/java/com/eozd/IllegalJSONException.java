package com.eozd;

/**
 * A simple class to denote JSON parsing errors.
 */
public class IllegalJSONException extends IllegalArgumentException {
    public IllegalJSONException(String msg) {
        super(msg);
    }
}
