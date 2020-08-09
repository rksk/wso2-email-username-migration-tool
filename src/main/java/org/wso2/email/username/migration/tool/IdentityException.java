package org.wso2.email.username.migration.tool;

public class IdentityException extends Exception {
    public IdentityException(String msg) {
        super(msg);
    }
    public IdentityException(String msg, Throwable e) {
        super(msg, e);
    }
}
