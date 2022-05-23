package Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenericMessageProtocol {
    private final List<List<String>> header = new ArrayList<>();
    private String body = "";

    public GenericMessageProtocol() {}

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
}
