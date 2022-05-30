package Membership;

import KVStore.Cluster;
import Storage.MembershipCounter;
import Storage.MembershipLog;
import Storage.MembershipLogEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MembershipView {
    private final MembershipLog membershipLog;
    private Cluster members;

    public MembershipView(MembershipLog membershipLog) {
        this.membershipLog = membershipLog;
        this.members = new Cluster();
        for (MembershipLogEntry entry : membershipLog.get()) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                members.add(entry.nodeId());
            }
        }
    }

    public Cluster getMembers() {
        return members;
    }

    public void updateMember(String nodeId, int membershipCounter) {
        try {
            membershipLog.log(new MembershipLogEntry(nodeId, membershipCounter));
        } catch (IOException e) {
            throw new RuntimeException("Couldn't write log to file.", e);
        }
        if (MembershipCounter.isJoin(membershipCounter)) {
            members.add(nodeId);
        } else {
            members.remove(nodeId);
        }
    }

    public void merge(List<String> members, List<MembershipLogEntry> log) {
        try {
            this.membershipLog.log(log);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't write log to file.", e);
        }

        //TODO recycle hashes as much as possible
        this.members = new Cluster(members);

        for (MembershipLogEntry entry : membershipLog.get()) {
            if (MembershipCounter.isJoin(entry.membershipCounter())) {
                if (!this.members.contains(entry.nodeId())) {
                    this.members.add(entry.nodeId());
                }
            } else {
                this.members.remove(entry.nodeId());
            }
        }
    }
}
