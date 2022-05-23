package Connection;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;

public class UnicastConnection implements AutoCloseable {
    private final PrintWriter writer;
    private final Socket socket;

    public UnicastConnection(String host, int port) throws IOException {
        socket = new Socket(host, port);
        writer = new PrintWriter(socket.getOutputStream());
    }

    public void send(String message) {
        writer.print(message);
        writer.flush();
    }

    public void close() throws IOException {
        socket.close();
    }
}
