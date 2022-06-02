package Message;

import KVStore.KVEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public static String put(KVEntry entry) {
        return new GenericMessageProtocol()
                .addHeaderEntry("PUT")
                .addHeaderEntry("key", entry.getKey())
                .setBody(entry.getValue())
                .toString();
    }

    public static String redirect(String node) {
        return new GenericMessageProtocol()
                .addHeaderEntry("REDIRECT")
                .addHeaderEntry("node", node)
                .toString();
    }

    public static class Get extends ClientServerMessageProtocol {
        private String key;
        public Get(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    public static class Delete extends ClientServerMessageProtocol {
        private String key;
        public Delete(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    public static class Put extends ClientServerMessageProtocol {
        private String key;
        private String value;
        public Put(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return this.key;
        }

        public String getValue() {
            return this.value;
        }
    }

    public static class Redirect extends ClientServerMessageProtocol {
        private String node;
        public Redirect(String node) {
            this.node = node;
        }

        public String getNode() {
            return this.node;
        }
    }

    public static ClientServerMessageProtocol parse(String message) throws MessageProtocolException {
        GenericMessageProtocol parsedMessage = new GenericMessageProtocol(message);
        List<List<String>> headers = GenericMessageProtocol.firstHeaderIsMessageType(parsedMessage.getHeaders());

        switch (parsedMessage.getHeaders().get(0).get(0)) {
            case "GET" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, List.of("key"));

                return new ClientServerMessageProtocol.Get(fields.get("key"));
            }
            case "DELETE" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, List.of("key"));

                return new ClientServerMessageProtocol.Delete(fields.get("key"));
            }
            case "PUT" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, List.of("key"));

                return new ClientServerMessageProtocol.Put(fields.get("key"), parsedMessage.getBody());
            }
            case "REDIRECT" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, List.of("node"));

                return new ClientServerMessageProtocol.Redirect(fields.get("node"));
            }
            default -> throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }
    }
}
