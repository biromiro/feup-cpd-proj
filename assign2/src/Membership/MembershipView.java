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
    //private int broadcasterIndex;
    private boolean isBroadcasting;

    public MembershipView(MembershipLog membershipLog, String currentNodeID) {
        this.membershipLog = membershipLog;
        this.currentNodeID = currentNodeID;
        this.cluster = new Cluster();
        for (MembershipLogEntry entry : membershipLog.get()) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                cluster.add(entry.nodeId());
            }
        }
        //this.broadcasterIndex = 0;
        this.isBroadcasting = false;
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

    public void merge(List<String> members, List<MembershipLogEntry> log) {
        // TODO recycle hashes as much as possible
        // TODO merge clusters
        this.cluster = new Cluster(members);
        this.merge(log);
    }

    public void merge(List<MembershipLogEntry> log) {
        for (MembershipLogEntry entry: this.membershipLog.log(log)) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                this.cluster.add(entry.nodeId());
            } else {
                this.cluster.remove(entry.nodeId());
            }
        }
    }


    public void startBroadcasting() { this.isBroadcasting = true; }
    public void stopBroadcasting() { this.isBroadcasting = false; }
    public boolean isBroadcasting() { return isBroadcasting; }

    /*
    public boolean isBroadcaster() {
        List<String> nodeIDs = this.getMembers().stream().sorted().toList();
        return nodeIDs.get(broadcasterIndex).equals(currentNodeID);
    }
    public void incrementBroadcasterIndex() { this.broadcasterIndex++; }
    public void setBroadcasterIndex(int broadcasterIndex) { this.broadcasterIndex = broadcasterIndex; }
    public int getBroadcasterIndex() { return broadcasterIndex; }
     */

    public String getNodeId() {
        return currentNodeID;
    }
}
