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

    public static class JoinMessage extends MembershipMessageProtocol {
        private String node;
        int membershipCounter;
        int port;
        JoinMessage(String node, int membershipCounter, int port) {
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

    public static class LeaveMessage extends MembershipMessageProtocol {
        private String node;
        int membershipCounter;
        LeaveMessage(String node, int membershipCounter) {
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

    public static class MembershipMessage extends MembershipMessageProtocol {
        List<String> members;
        List<MembershipLogEntry> log;
        MembershipMessage(List<String> members, List<MembershipLogEntry> log) {
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

    public static class ReinitializeMessage extends MembershipMessageProtocol {
        private String node;
        int port;
        ReinitializeMessage(String node, int port) {
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


    private static Map<String, String> parseBinaryHeaders(List<List<String>> headers) throws MessageProtocolException {
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

    private static void ensureOnlyContains(Map<String, String> fields, List<String> keys)
            throws MessageProtocolException {
        for (String key : keys) {
            System.out.println("CHECKING " + key);
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
                Map<String, String> fields = parseBinaryHeaders(headers);
                System.out.println("FIELDS: " + fields);
                ensureOnlyContains(fields, Arrays.asList("node", "counter", "port"));

                return new JoinMessage(
                        fields.get("node"),
                        Integer.parseInt(fields.get("counter")),
                        Integer.parseInt(fields.get("port"))
                );
            }
            case "LEAVE" -> {
                Map<String, String> fields = parseBinaryHeaders(headers);
                ensureOnlyContains(fields, List.of("node", "counter"));

                return new LeaveMessage(
                        fields.get("node"),
                        Integer.parseInt(fields.get("counter"))
                );
            }
            case "MEMBERSHIP" -> {
                Map<String, String> fields = parseBinaryHeaders(headers);
                ensureOnlyContains(fields, Arrays.asList("members", "log"));

                int membersCount = Integer.parseInt(fields.get("members"));
                int logCount = Integer.parseInt(fields.get("log"));

                List<String> members = parsedMessage
                        .getBody()
                        .lines()
                        .limit(membersCount)
                        .collect(Collectors.toList());
                List<MembershipLogEntry> log = parseLogEntries(parsedMessage.getBody(), membersCount, logCount);

                return new MembershipMessage(members, log);
            }
            default -> throw new MessageProtocolException("Unknown message '"
                    + String.join(" ", parsedMessage.getHeaders().get(0)) + '\'');
        }
    }
}
