package Message;

import java.util.*;
import java.util.stream.Collectors;

public class GenericMessageProtocol {
    private final List<List<String>> header;
    private String body = "";

    public GenericMessageProtocol() {
        header = new ArrayList<>();
    }
    public GenericMessageProtocol(String message) {
        header = message.lines()
                .takeWhile(line -> !line.isEmpty())
                .map(line -> Arrays.asList(line.split(" ")))
                .collect(Collectors.toList());

        body = message.substring(message.indexOf("\n\n") + 2);
    }

    public GenericMessageProtocol addHeaderEntry(String ... fields) {
        header.add(Arrays.asList(fields));
        return this;
    }

    public GenericMessageProtocol addHeaderEntry(List<String> fields) {
        header.add(fields);
        return this;
    }

    public GenericMessageProtocol setBody(String body) {
        this.body = body;
        return this;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (List<String> headerEntry : header) {
            builder.append(String.join(" ", headerEntry));
            builder.append("\n");
        }
        builder.append("\n");
        builder.append(body);

        return builder.toString();
    }

    public List<List<String>> getHeaders() {
        return header;
    }

    public String getBody() {
        return body;
    }

    public static Map<String, String> parseBinaryHeaders(List<List<String>> headers) throws MessageProtocolException {
        Map<String, String> fields = new HashMap<>();
        for (List<String> header: headers) {
            if (header.size() != 2) {
                throw new MessageProtocolException("Unexpected header '"
                        + String.join(" ", header) + '\'');
            }
            if (fields.containsKey(header.get(0))) {
                throw new MessageProtocolException("Duplicate header '"
                        + String.join(" ", header) + '\'');
            }
            fields.put(header.get(0), header.get(1));
        }
        return fields;
    }

    public static void ensureOnlyContains(Map<String, String> fields, List<String> keys)
            throws MessageProtocolException {
        for (String key : keys) {
            if (!fields.containsKey(key)) {
                throw new MessageProtocolException("Missing field '" + key + '\'');
            }
        }

        Optional<String> others = fields
                .keySet()
                .stream()
                .filter(k -> !keys.contains(k))
                .findAny();
        if (others.isPresent()) {
            throw new MessageProtocolException("Unexpected field '" + others.get() + '\'');
        }
    }
}
