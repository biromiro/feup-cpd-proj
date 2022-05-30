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
                .addHeaderEntry("log", String.valueOf(membershipView.getLog().size()))
                .setBody(builder.toString())
                .toString();
    }

    public static class Join extends MembershipMessageProtocol {
        private final String node;
        private int membershipCounter;
        private int port;
        public Join(String node, int membershipCounter, int port) {
            System.out.println("MAKE JOIN MESSAGE");
            this.node = node;
            this.membershipCounter = membershipCounter;
            this.port = port;
            System.out.println("MADE JOIN MESSAGE");
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
        private int membershipCounter;
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
        private List<String> members;
        private List<MembershipLogEntry> log;
        public Membership(List<String> members, List<MembershipLogEntry> log) {
            this.members = members;
            this.log = log;
        }

        public List<String> getMembers() {
            return members;
        }

        public List<MembershipLogEntry> getLog() {
            return log;
        }
    }

    public static class Reinitialize extends MembershipMessageProtocol {
        private final String node;
        private int port;
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

    public static MembershipMessageProtocol parse(String message) throws MessageProtocolException {
        GenericMessageProtocol parsedMessage = new GenericMessageProtocol(message);
        if (parsedMessage.getHeaders().size() == 0) {
            System.out.println("WHAT");
            throw new MessageProtocolException("Message is missing headers");
        }
        if (parsedMessage.getHeaders().get(0).size() != 1) {
            System.out.println("WHAT 2???");
            throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }

        List<List<String>> headers = parsedMessage
                .getHeaders()
                .subList(1, parsedMessage.getHeaders().size());

        System.out.println("HEADERS: " + headers);
        System.out.println(parsedMessage.getHeaders());
        System.out.println(parsedMessage.getHeaders().get(0));
        System.out.println(parsedMessage.getHeaders().get(0).get(0));

        switch (parsedMessage.getHeaders().get(0).get(0)) {
            case "JOIN", "REINITIALIZE" -> {
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(headers);
                System.out.println("FIELDS: " + fields);
                GenericMessageProtocol.ensureOnlyContains(fields, Arrays.asList("node", "counter", "port"));

                return new Join(
                        fields.get("node"),
                        Integer.parseInt(fields.get("counter")),
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
                GenericMessageProtocol.ensureOnlyContains(fields, Arrays.asList("members", "log"));

                int membersCount = Integer.parseInt(fields.get("members"));
                int logCount = Integer.parseInt(fields.get("log"));

                List<String> members = parsedMessage
                        .getBody()
                        .lines()
                        .limit(membersCount)
                        .collect(Collectors.toList());
                List<MembershipLogEntry> log = parseLogEntries(parsedMessage.getBody(), membersCount, logCount);

                return new Membership(members, log);
            }
            default -> throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }
    }
}
