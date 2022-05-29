package Message;

public class MessageProtocolException extends Exception {
    public MessageProtocolException(String message) {
        super(message);
    }

    public MessageProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
