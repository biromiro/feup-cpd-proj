package Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;

public class UnicastConnection implements AutoCloseable {
    private final PrintWriter writer;
    private final BufferedReader reader;
    private final Socket socket;

    public UnicastConnection(String host, int port) throws IOException {
        socket = new Socket(host, port);
        writer = new PrintWriter(socket.getOutputStream());
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    public void send(String message) {
        writer.print(message);
        writer.flush();
    }

    public String read() throws IOException {
        String line;
        StringBuilder content = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            content.append(line);
        }
        return content.toString();
    }

    public void close() throws IOException {
        socket.close();
    }
}
