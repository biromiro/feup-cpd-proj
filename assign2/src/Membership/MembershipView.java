package Membership;

import KVStore.Cluster;
import Storage.MembershipCounter;
import Storage.MembershipLog;
import Storage.MembershipLogEntry;

import java.io.IOException;
import java.util.List;

public class MembershipView {
    private final MembershipLog membershipLog;
    private Cluster cluster;
    private int priority;

    public MembershipView(MembershipLog membershipLog) {
        this.membershipLog = membershipLog;
        this.cluster = new Cluster();
        for (MembershipLogEntry entry : membershipLog.get()) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                cluster.add(entry.nodeId());
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public List<String> getMembers() {
        return cluster.getMembers();
    }

    public List<MembershipLogEntry> getLog() { return membershipLog.get(); }
    public void updateMember(String nodeId, int membershipCounter) {
        try {
            membershipLog.log(new MembershipLogEntry(nodeId, membershipCounter));
        } catch (IOException e) {
            throw new RuntimeException("Couldn't write log to file.", e);
        }
        if (MembershipCounter.isJoin(membershipCounter)) {
            cluster.add(nodeId);
        } else {
            cluster.remove(nodeId);
        }
    }

    public void merge(List<String> members, List<MembershipLogEntry> log) {
        try {
            this.membershipLog.log(log);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't write log to file.", e);
        }

        //TODO recycle hashes as much as possible
        this.cluster = new Cluster(members);

        for (MembershipLogEntry entry : membershipLog.get()) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                if (!this.cluster.contains(entry.nodeId())) {
                    this.cluster.add(entry.nodeId());
                }
            } else {
                this.cluster.remove(entry.nodeId());
            }
        }
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return this.priority;
    }
}
