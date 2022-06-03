package Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.stream.Collectors;

public class TcpConnection implements AutoCloseable {
    private final PrintWriter writer;
    private final BufferedReader reader;
    private final Socket socket;
    private static final int TIMEOUT = 5000;

    public TcpConnection(String host, int port) throws IOException {
        //socket = new Socket(host, port);
        // TODO REMOVE HACK
        socket = new Socket(host, port + Integer.parseInt(host.split("\\.")[3]));
        socket.setSoTimeout(TIMEOUT);
        writer = new PrintWriter(socket.getOutputStream());
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    public String getHost() {
        return socket.getInetAddress().getHostAddress();
    }

    public int getPort() {
        return socket.getPort();
    }

    public void send(String message) {
        writer.print(message);
        writer.flush();
    }

    public String read() {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
    }

    @Override
    public void close() throws IOException {
        reader.close();
        writer.close();
        socket.close();
    }
}
