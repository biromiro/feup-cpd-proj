package Storage;

import java.util.List;

public record MembershipLogEntry(String nodeId, int membershipCounter) {
    @Override
    public String toString() {
        return nodeId + " " + membershipCounter;
    }
}
