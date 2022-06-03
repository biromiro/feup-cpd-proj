package Membership;

import KVStore.BucketTransferrer;
import KVStore.Cluster;
import Storage.Bucket;
import Storage.MembershipCounter;
import Storage.MembershipLog;
import Storage.MembershipLogEntry;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class MembershipView {
    private final MembershipLog membershipLog;
    private final String currentNodeID;
    private Cluster cluster;
    private MulticasterState multicasterState;
    private final BucketTransferrer transferrer;
    private ScheduledFuture<?> task;

    public MembershipView(MembershipLog membershipLog, String currentNodeID, Cluster cluster, BucketTransferrer transferrer) {
        this.membershipLog = membershipLog;
        this.currentNodeID = currentNodeID;
        this.cluster = cluster;
        this.transferrer = transferrer;

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
            if (cluster.add(nodeId) && !nodeId.equals(currentNodeID)) {
                transferrer.transfer(nodeId);
            }
        } else {
            cluster.remove(nodeId);
        }
    }

    public boolean merge(List<String> members, List<MembershipLogEntry> log) {
        // TODO recycle hashes as much as possible
        // TODO merge clusters
        this.cluster.replace(members);
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

    public synchronized void becomeMulticasterCandidate(ScheduledFuture<?> task) {
        if (this.task != null) {
            this.task.cancel(true);
        }
        this.task = task;
        this.multicasterState = MulticasterState.MULTICASTING_CANDIDATE;
    }
    public synchronized void startMulticasting() {
        if (this.task != null && this.multicasterState == MulticasterState.MULTICASTING_CANDIDATE) {
            this.multicasterState = MulticasterState.MULTICASTING_MEMBERSHIP;
        }
    }
    public synchronized void stopMulticasting() {
        if (this.task != null) {
            this.task.cancel(true);
        }
        this.task = null;
        this.multicasterState = MulticasterState.NOT_MULTICASTING_MEMBERSHIP;
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

    public Integer getCount() {
        return membershipLog.get().stream().map(MembershipLogEntry::membershipCounter).reduce(0, Integer::sum);
    }

    public void reinitialize(String nodeId) {
        if (!nodeId.equals(currentNodeID)) {
            this.transferrer.reinitialize(nodeId);
        }
    }
}
