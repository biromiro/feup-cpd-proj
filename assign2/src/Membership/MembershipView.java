package Membership;

import KVStore.Cluster;
import Storage.MembershipCounter;
import Storage.MembershipLog;
import Storage.MembershipLogEntry;

import java.util.List;

public class MembershipView {
    private final MembershipLog membershipLog;
    private final String currentNodeID;
    private Cluster cluster;
    private MulticasterState multicasterState;

    public MembershipView(MembershipLog membershipLog, String currentNodeID) {
        this.membershipLog = membershipLog;
        this.currentNodeID = currentNodeID;
        this.cluster = new Cluster();
        for (MembershipLogEntry entry : membershipLog.get()) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                cluster.add(entry.nodeId());
            }
        }

        this.multicasterState = MulticasterState.NOT_MULTICASTING_MEMBERSHIP;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public List<String> getMembers() {
        return cluster.getMembers();
    }

    public List<MembershipLogEntry> getLog() { return membershipLog.get(); }
    public void updateMember(String nodeId, int membershipCounter) {
        int counter = membershipLog.log(new MembershipLogEntry(nodeId, membershipCounter));

        if (MembershipCounter.isJoin(counter)) {
            cluster.add(nodeId);
        } else {
            cluster.remove(nodeId);
        }
    }

    public boolean merge(List<String> members, List<MembershipLogEntry> log) {
        // TODO recycle hashes as much as possible
        // TODO merge clusters
        this.cluster = new Cluster(members);
        return this.merge(log);
    }

    public boolean merge(List<MembershipLogEntry> log) {
        boolean receivedOutdated = this.membershipLog.receivedOutdated(log);
        for (MembershipLogEntry entry: this.membershipLog.log(log)) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                this.cluster.add(entry.nodeId());
            } else {
                this.cluster.remove(entry.nodeId());
            }
        }
        return receivedOutdated;
    }

    public int getIndexInCluster() {
        List<String> nodeIDs = this.getMembers().stream().sorted().toList();
        return nodeIDs.indexOf(this.currentNodeID);
    }

    public void startMulticasting() {
        this.multicasterState = MulticasterState.MULTICASTING_MEMBERSHIP;
    }
    public void stopMulticasting() {
        this.multicasterState = MulticasterState.NOT_MULTICASTING_MEMBERSHIP;
    }
    public void becomeMulticasterCandidate() {
        this.multicasterState = MulticasterState.MULTICASTING_CANDIDATE;
    }
    public boolean isMulticasting() {
        return multicasterState == MulticasterState.MULTICASTING_MEMBERSHIP;
    }
    public boolean mayMulticast() {
        return multicasterState == MulticasterState.MULTICASTING_CANDIDATE
                || multicasterState == MulticasterState.MULTICASTING_MEMBERSHIP;
    }

    public String getNodeId() {
        return currentNodeID;
    }
}
