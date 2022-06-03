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

    public static String redirect(List<String> targets) {
        GenericMessageProtocol message = new GenericMessageProtocol().addHeaderEntry("REDIRECT");
        for (String target : targets) {
            message.addHeaderEntry("redirect", target);
        }

        return message.toString();
    }

    public static String done() {
        return new GenericMessageProtocol()
                .addHeaderEntry("DONE")
                .toString();
    }
    public static String done(String content) {
        return new GenericMessageProtocol()
                .addHeaderEntry("DONE")
                .setBody(content)
                .toString();
    }

    public static String error(String errorMessage) {
        return new GenericMessageProtocol()
                .addHeaderEntry("ERROR")
                .setBody(errorMessage)
                .toString();
    }

    public static class Get extends ClientServerMessageProtocol {
        private final String key;
        public Get(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    public static class Delete extends ClientServerMessageProtocol {
        private final String key;
        public Delete(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    public static class Put extends ClientServerMessageProtocol {
        private final String key;
        private final String value;
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
        private final List<String> hosts;

        public Redirect(List<String> targets) {
            this.hosts = targets;
        }
        public List<String> getHosts() {
            return hosts;
        }
    }

    public static class Done extends ClientServerMessageProtocol {
        private final String body;
        public Done(String body) {
            this.body = body;
        }

        public String getBody() {
            return body;
        }
    }

    public static class Error extends ClientServerMessageProtocol {
        private final String errorMessage;

        public Error(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    public static ClientServerMessageProtocol parse(String message) throws MessageProtocolException {
        GenericMessageProtocol parsedMessage = new GenericMessageProtocol(message);
        List<List<String>> headers = GenericMessageProtocol.firstHeaderIsMessageType(parsedMessage.getHeaders());

        switch (parsedMessage.getHeaders().get(0).get(1)) {
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
                List<List<String>> fields = headers.stream()
                        .filter(header -> header.get(0).equals("redirect")).toList();
                List<String> targets = parseRedirectHeaders(fields);

                return new ClientServerMessageProtocol.Redirect(targets);
            }
            case "DONE" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, new ArrayList<>());

                return new ClientServerMessageProtocol.Done(parsedMessage.getBody());
            }
            case "ERROR" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, new ArrayList<>());

                return new ClientServerMessageProtocol.Error(parsedMessage.getBody());
            }
            default -> throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }
    }

    private static List<String> parseRedirectHeaders(List<List<String>> redirectHeaders) throws MessageProtocolException {
        List<String> targets = new ArrayList<>();
        for (List<String> header : redirectHeaders) {
            if (header.size() != 2) {
                throw new MessageProtocolException("Unexpected redirect header '" + header + '\'');
            }
            String target = header.get(1);
            targets.add(target);
        }
        return targets;
    }
}