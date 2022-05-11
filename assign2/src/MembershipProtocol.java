import java.io.IOException;
import java.net.*;
import java.util.List;

public class MembershipProtocol {
    private final String host;
    private final int port;

    public MembershipProtocol(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private void sendMulticast(String message) throws IOException {
        InetAddress group = InetAddress.getByName(host);
        NetworkInterface local = NetworkInterface.getByName("lo");

        MulticastSocket socket = new MulticastSocket();
        socket.joinGroup(new InetSocketAddress(host, port), local);
        socket.send(new DatagramPacket(message.getBytes(), message.length(), group, port));
        socket.close();
    }

    public void join(int membershipCounter) throws IOException {
        String builder = "JOIN\n" +
                "counter " + membershipCounter +
                "\n\n";
        sendMulticast(builder);
    }

    public void leave(int membershipCounter) throws IOException {
        String builder = "LEAVE\n" +
                "counter " + membershipCounter +
                "\n\n";
        sendMulticast(builder);
    }

    public void membership(List<String> members, List<MembershipLogEntry> log) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("MEMBERSHIP\n")
                .append("members ").append(members.size()).append("\n")
                .append("log ").append(log.size())
                .append("\n\n");

        for (String member : members) {
            builder.append(member).append("\n");
        }
        builder.append("\n");
        for (MembershipLogEntry entry : log) {
            builder.append(entry.toString()).append("\n");
        }

        // TODO send but not multicast
    }
}
