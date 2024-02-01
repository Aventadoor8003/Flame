package cis5550.exception;

public class TableRenameException extends RuntimeException {
    public TableRenameException(String message) {
        super(message);
    }

    public TableRenameException(String message, Throwable cause) {
        super(message, cause);
    }
}
