package Message;

import Membership.MembershipView;
import Storage.MembershipLogEntry;

import java.util.*;
import java.util.stream.Collectors;

public class MembershipMessageProtocol {
    public static String join(String nodeId, int port, int membershipCounter, Map<String, Integer> blacklist) {
        
        GenericMessageProtocol genericMessageProtocol = new GenericMessageProtocol()
                .addHeaderEntry("JOIN")
                .addHeaderEntry("node", nodeId)
                .addHeaderEntry("port", String.valueOf(port))
                .addHeaderEntry("counter", String.valueOf(membershipCounter));
        
        for (Map.Entry<String, Integer> entry : blacklist.entrySet()) {
            genericMessageProtocol.addHeaderEntry("blacklist", entry.getKey(), String.valueOf(entry.getValue()));
        }
        
        return genericMessageProtocol.toString();
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
        int logSize = Math.min(32, membershipView.getLog().size());
        for (MembershipLogEntry entry : membershipView.getLog().subList(0, logSize)) {
            builder.append(entry.toString()).append("\n");
        }

        return new GenericMessageProtocol()
                .addHeaderEntry("MEMBERSHIP")
                .addHeaderEntry("node", membershipView.getNodeId())
                .addHeaderEntry("members", String.valueOf(membershipView.getMembers().size()))
                .addHeaderEntry("log", String.valueOf(logSize))
                .setBody(builder.toString())
                .toString();
    }

    public static String reinitialize(String nodeId, int port, Map<String, Integer> blacklist) {
        GenericMessageProtocol genericMessageProtocol = new GenericMessageProtocol()
                .addHeaderEntry("REINITIALIZE")
                .addHeaderEntry("node", nodeId)
                .addHeaderEntry("port", String.valueOf(port));

        for (Map.Entry<String, Integer> entry : blacklist.entrySet()) {
            genericMessageProtocol.addHeaderEntry("blacklist", entry.getKey(), String.valueOf(entry.getValue()));
        }

        return genericMessageProtocol.toString();
    }

    public static String membershipLog(MembershipView membershipView) {
        StringBuilder builder = new StringBuilder();

        int logSize = Math.min(32, membershipView.getLog().size());
        for (MembershipLogEntry entry : membershipView.getLog().subList(0, logSize)) {
            builder.append(entry.toString()).append("\n");
        }

        return new GenericMessageProtocol()
                .addHeaderEntry("MEMBERSHIP")
                .addHeaderEntry("node", membershipView.getNodeId())
                .addHeaderEntry("log", String.valueOf(logSize))
                .setBody(builder.toString())
                .toString();
    }

    public static class Join extends MembershipMessageProtocol {
        private final String node;
        private final int membershipCounter;
        private final int port;

        private final Map<String, Integer> blacklist;
        public Join(String node, int membershipCounter, int port, Map<String, Integer> blacklist) {
            this.node = node;
            this.membershipCounter = membershipCounter;
            this.port = port;
            this.blacklist = blacklist;
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

        public Map<String, Integer> getBlacklist() {
            return blacklist;
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

        private final Map<String, Integer> blacklist;

        public Reinitialize(String node, int port, Map<String, Integer> blacklist) {
            this.node = node;
            this.port = port;
            this.blacklist = blacklist;
        }

        public String getId() {
            return node;
        }

        public int getPort() {
            return port;
        }

        public Map<String, Integer> getBlacklist() {
            return blacklist;
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
                List<List<String>> blacklistHeaders = headers.stream()
                        .filter(header -> header.get(0).equals("blacklist")).toList();
                List<List<String>> otherHeaders = headers.stream()
                        .filter(header -> !header.get(0).equals("blacklist")).toList();
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(otherHeaders);
                Map<String, Integer> blacklist = parseBlacklistHeaders(blacklistHeaders);

                GenericMessageProtocol.ensureOnlyContains(fields, Arrays.asList("node", "counter", "port"));

                return new Join(
                        fields.get("node"),
                        Integer.parseInt(fields.get("counter")),
                        Integer.parseInt(fields.get("port")),
                        blacklist
                );
            }
            case "REINITIALIZE" -> {
                List<List<String>> blacklistHeaders = headers.stream()
                        .filter(header -> header.get(0).equals("blacklist")).toList();
                List<List<String>> otherHeaders = headers.stream()
                        .filter(header -> !header.get(0).equals("blacklist")).toList();
                Map<String, String> fields = GenericMessageProtocol.parseBinaryHeaders(otherHeaders);
                Map<String, Integer> blacklist = parseBlacklistHeaders(blacklistHeaders);

                GenericMessageProtocol.ensureOnlyContains(fields, Arrays.asList("node", "port"));

                return new Reinitialize(
                        fields.get("node"),
                        Integer.parseInt(fields.get("port")),
                        blacklist
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

    private static Map<String, Integer> parseBlacklistHeaders(List<List<String>> blacklistHeaders) throws MessageProtocolException {
        Map<String, Integer> blacklist = new HashMap<>();

        for (List<String> header : blacklistHeaders) {
            if (header.size() != 3) {
                throw new MessageProtocolException("Unexpected blacklist header '" + header + '\'');
            }
            String key = header.get(1);
            String value = header.get(2);
            try {
                blacklist.put(key, Integer.parseInt(value));
            } catch (NumberFormatException e) {
                throw new MessageProtocolException("Unexpected blacklist entry '" + key + " " + value
                        + "'. Expected integer, got " + value);
            }
        }
        return blacklist;
    }
}
