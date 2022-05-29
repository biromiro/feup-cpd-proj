package Connection;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class MulticastConnection implements AutoCloseable {
    private static final int BUFFER_SIZE = 1024;
    private final int port;
    private final InetAddress group;
    private final MulticastSocket socket;

    public MulticastConnection(String host, int port) throws IOException {
        this.port = port;
        group = InetAddress.getByName(host);
        socket = new MulticastSocket(port);
        socket.joinGroup(new InetSocketAddress(host, port), NetworkInterface.getByName("lo"));
    }

    public void send(String message) throws IOException {
        System.out.println("sending " +  group + " " +  port);
        socket.send(new DatagramPacket(message.getBytes(), message.length(), group, port));
    }

    public String receive() throws IOException {
        byte[] byteArray = new byte[BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(byteArray, byteArray.length);
        System.out.println("waiting " +  group + " " +  port);
        socket.receive(packet);
        System.out.println("received");

        return Arrays.toString(packet.getData());
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    public void close() throws IOException {
        socket.close();
    }
}
