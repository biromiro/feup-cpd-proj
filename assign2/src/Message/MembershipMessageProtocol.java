package Message;

import Membership.MembershipView;
import Storage.MembershipLogEntry;

import java.util.*;
import java.util.stream.Collectors;

public class MembershipMessageProtocol {
    public static String join(String nodeId, int port, int membershipCounter) {
        return new GenericMessageProtocol()
                .addHeaderEntry("JOIN")
                .addHeaderEntry("node", nodeId)
                .addHeaderEntry("port", String.valueOf(port))
                .addHeaderEntry("counter", String.valueOf(membershipCounter))
                .toString();
    }

    public static String leave(String nodeId, int membershipCounter) {
        return new GenericMessageProtocol()
                .addHeaderEntry("LEAVE")
                .addHeaderEntry("node", nodeId)
                .addHeaderEntry("counter", String.valueOf(membershipCounter))
                .toString();
    }

    public static String membership(MembershipView membershipView) {
        StringBuilder builder = new StringBuilder();

        for (String member : membershipView.getMembers()) {
            builder.append(member).append("\n");
        }
        builder.append("\n");
        for (MembershipLogEntry entry : membershipView.getLog()) {
            builder.append(entry.toString()).append("\n");
        }

        return new GenericMessageProtocol()
                .addHeaderEntry("MEMBERSHIP")
                .addHeaderEntry("node", membershipView.getNodeId())
                .addHeaderEntry("members", String.valueOf(membershipView.getMembers().size()))
                .addHeaderEntry("log", String.valueOf(membershipView.getLog().size()))
                .setBody(builder.toString())
                .toString();
    }

    public static String reinitialize(String nodeId, int port) {
        return new GenericMessageProtocol()
                .addHeaderEntry("REINITIALIZE")
                .addHeaderEntry("node", nodeId)
                .addHeaderEntry("port", String.valueOf(port))
                .toString();
    }

    public static String membershipLog(MembershipView membershipView) {
        StringBuilder builder = new StringBuilder();

        for (MembershipLogEntry entry : membershipView.getLog()) {
            builder.append(entry.toString()).append("\n");
        }

        return new GenericMessageProtocol()
                .addHeaderEntry("MEMBERSHIP")
                .addHeaderEntry("node", membershipView.getNodeId())
                .addHeaderEntry("log", String.valueOf(membershipView.getLog().size()))
                .setBody(builder.toString())
                .toString();
    }

    public static class Join extends MembershipMessageProtocol {
        private final String node;
        private final int membershipCounter;
        private final int port;
        public Join(String node, int membershipCounter, int port) {
            this.node = node;
            this.membershipCounter = membershipCounter;
            this.port = port;
        }

        public String getId() {
            return node;
        }

        public int getMembershipCounter() {
            return membershipCounter;
        }

        public int getPort() {
            return port;
        }
    }

    public static class Leave extends MembershipMessageProtocol {
        private final String node;
        private final int membershipCounter;
        public Leave(String node, int membershipCounter) {
            this.node = node;
            this.membershipCounter = membershipCounter;
        }

        public String getId() {
            return node;
        }

        public int getMembershipCounter() {
            return membershipCounter;
        }
    }

    public static class Membership extends MembershipMessageProtocol {
        private final String node;
        private final List<String> members;
        private final List<MembershipLogEntry> log;
        public Membership(String node, List<String> members, List<MembershipLogEntry> log) {
            this.node = node;
            this.members = members;
            this.log = log;
        }

        public Membership(String node, List<MembershipLogEntry> log) {
            this(node, null, log);
        }

        public List<String> getMembers() {
            return members;
        }

        public List<MembershipLogEntry> getLog() {
            return log;
        }

        public String getId() { return node; }
    }

    public static class Reinitialize extends MembershipMessageProtocol {
        private final String node;
        private final int port;
        public Reinitialize(String node, int port) {
            this.node = node;
            this.port = port;
        }

        public String getId() {
            return node;
        }

        public int getPort() {
            return port;
        }
    }

    private static List<MembershipLogEntry> parseLogEntries(String body, int membersCount, int logCount)
            throws MessageProtocolException {
        List<String> logList = body
                .lines()
                .skip(membersCount + 1)
                .limit(logCount)
                .collect(Collectors.toList());
        List<MembershipLogEntry> log = new ArrayList<>();
        for (String line: logList) {
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split(" ");
            if (parts.length != 2) {
                throw new MessageProtocolException("Unexpected log entry '" + line + '\'');
            }
            try {
                log.add(new MembershipLogEntry(parts[0], Integer.parseInt(parts[1])));
            } catch (NumberFormatException e) {
                throw new MessageProtocolException("Unexpected log entry '" + line
                        + "'. Expected integer, got " + parts[1]);
            }
        }
        return log;
    }

    private static List<MembershipLogEntry> parseLogEntries(String body, int logCount)
            throws MessageProtocolException {
        return parseLogEntries(body, -1, logCount);
    }

    public static MembershipMessageProtocol parse(String message) throws MessageProtocolException {
        GenericMessageProtocol parsedMessage = new GenericMessageProtocol(message);
        List<List<String>> headers = GenericMessageProtocol.firstHeaderIsMessageType(parsedMessage.getHeaders());

        switch (parsedMessage.getHeaders().get(0).get(0)) {
            case "JOIN" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, Arrays.asList("node", "counter", "port"));

                return new Join(
                        fields.get("node"),
                        Integer.parseInt(fields.get("counter")),
                        Integer.parseInt(fields.get("port"))
                );
            }
            case "REINITIALIZE" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, Arrays.asList("node", "port"));

                return new Reinitialize(
                        fields.get("node"),
                        Integer.parseInt(fields.get("port"))
                );
            }
            case "LEAVE" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, List.of("node", "counter"));

                return new Leave(
                        fields.get("node"),
                        Integer.parseInt(fields.get("counter"))
                );
            }
            case "MEMBERSHIP" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                GenericMessageProtocol.ensureOnlyContains(fields, List.of("node", "log"), List.of("members"));

                int logCount = Integer.parseInt(fields.get("log"));


                if (fields.containsKey("members")) {
                    int membersCount = Integer.parseInt(fields.get("members"));
                    List<String> members = parsedMessage
                            .getBody()
                            .lines()
                            .limit(membersCount)
                            .collect(Collectors.toList());

                    List<MembershipLogEntry> log = parseLogEntries(parsedMessage.getBody(), membersCount, logCount);

                    return new Membership(fields.get("node"), members, log);
                } else {
                    List<MembershipLogEntry> log = parseLogEntries(parsedMessage.getBody(), logCount);
                    return new Membership(fields.get("node"), log);
                }
            }
            default -> throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }
    }
}
