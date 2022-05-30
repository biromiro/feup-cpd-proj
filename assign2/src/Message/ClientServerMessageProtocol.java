package Message;

import Storage.MembershipLogEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public static ClientServerMessageProtocol parse(String message) throws MessageProtocolException {
        GenericMessageProtocol parsedMessage = new GenericMessageProtocol(message);
        if (parsedMessage.getHeaders().size() == 0) {
            throw new MessageProtocolException("Message is missing headers");
        }
        if (parsedMessage.getHeaders().get(0).size() != 1) {
            throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }

        List<List<String>> headers = parsedMessage
                .getHeaders()
                .subList(1, parsedMessage.getHeaders().size());

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
                GenericMessageProtocol.ensureOnlyContains(fields, new ArrayList<>());

                return new ClientServerMessageProtocol.Put(fields.get("key"), parsedMessage.getBody());
            }
            default -> throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }
    }
}
