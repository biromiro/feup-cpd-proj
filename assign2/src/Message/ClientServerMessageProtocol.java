package Message;

public class ClientServerMessageProtocol {
    public static String get(String key) {
        return new GenericMessageProtocol()
                .addHeaderEntry("GET")
                .addHeaderEntry("key", key)
                .toString();
    }

    public static String delete(String key) {
        return new GenericMessageProtocol()
                .addHeaderEntry("DELETE")
                .addHeaderEntry("key", key)
                .toString();
    }

    public static String put(String key, String value) {
        return new GenericMessageProtocol()
                .addHeaderEntry("GET")
                .addHeaderEntry("key", key)
                .setBody(value)
                .toString();
    }
}
