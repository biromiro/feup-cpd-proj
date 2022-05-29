package Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
}
