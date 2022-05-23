package Message;

import java.util.List;

public class MembershipMessageProtocol {
    public static String join(int membershipCounter, int port) {
        return new GenericMessageProtocol()
                .addHeaderEntry("JOIN")
                .addHeaderEntry("port", String.valueOf(port))
                .addHeaderEntry("counter", String.valueOf(membershipCounter))
                .toString();
    }

    public static String leave(int membershipCounter) {
        return new GenericMessageProtocol()
                .addHeaderEntry("LEAVE")
                .addHeaderEntry("counter", String.valueOf(membershipCounter))
                .toString();
    }

    public static String membership(List<String> members, List<MembershipLogEntry> log) {
        StringBuilder builder = new StringBuilder();

        for (String member : members) {
            builder.append(member).append("\n");
        }
        builder.append("\n");
        for (MembershipLogEntry entry : log) {
            builder.append(entry.toString()).append("\n");
        }

        return new GenericMessageProtocol()
                .addHeaderEntry("MEMBERSHIP")
                .addHeaderEntry("members", String.valueOf(members.size()))
                .addHeaderEntry("log", String.valueOf(log.size()))
                .setBody(builder.toString())
                .toString();
    }
}
