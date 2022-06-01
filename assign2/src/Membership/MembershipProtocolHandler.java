package Membership;

import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.util.concurrent.ThreadPoolExecutor;

public class MembershipProtocolHandler implements Runnable {
    private static final int MEMBERSHIP_THRESHOLD_SEND = 5;
    private final String receivedMessage;
    private final MembershipView membershipView;
    private final ThreadPoolExecutor executor;
    private final MembershipHandler membershipHandler;

    public MembershipProtocolHandler(String receivedMessage, MembershipView membershipView, ThreadPoolExecutor executor,
                                     MembershipHandler membershipHandler) {
        this.receivedMessage = receivedMessage;
        this.membershipView = membershipView;
        this.executor = executor;
        this.membershipHandler = membershipHandler;
    }

    @Override
    public void run() {
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            e.printStackTrace();
            return;
        }

        if (parsedMessage instanceof MembershipMessageProtocol.Join joinMessage) {
            if (!joinMessage.getId().equals(membershipView.getNodeId())) {
                handleJoin(joinMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.Leave leaveMessage) {
            if (!leaveMessage.getId().equals(membershipView.getNodeId())) {
                handleLeave(leaveMessage);
            }
        } else if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            if (!membershipMessage.getId().equals(membershipView.getNodeId())) {
                handleMembership(membershipMessage);
            }

        } else if (parsedMessage instanceof MembershipMessageProtocol.Reinitialize reinitializeMessage) {
            if (!reinitializeMessage.getId().equals(membershipView.getNodeId())) {
                handleReinitialize(reinitializeMessage);
            }
        }
    }

    private void sendMembershipView(String host, int port) {
        if (membershipView.isMulticasting()) {
            executor.submit(new MembershipMessageDispatcher(executor, membershipView, host, port));
        } else {
            double probability = Math.min(1.0,
                    (double) MEMBERSHIP_THRESHOLD_SEND / (membershipView.getCluster().size() - 1));
            if (Math.random() < probability) {
                executor.submit(new MembershipMessageDispatcher(executor, membershipView, host, port));
            }
        }
    }

    private void handleJoin(MembershipMessageProtocol.Join joinMessage) {
        membershipView.updateMember(joinMessage.getId(), joinMessage.getMembershipCounter());

        this.sendMembershipView(joinMessage.getId(), joinMessage.getPort());
    }

    private void handleLeave(MembershipMessageProtocol.Leave leaveMessage) {
        membershipView.updateMember(leaveMessage.getId(), leaveMessage.getMembershipCounter());
        // TODO
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getLog());

        int idCompare = membershipMessage.getId().compareTo(membershipView.getNodeId());
        if (idCompare < 0) {
            membershipView.stopMulticasting();
        }
        // TODO
    }

    private void handleReinitialize(MembershipMessageProtocol.Reinitialize reinitializeMessage) {
        // TODO maybe should send the membership counter as well on reinitialize message?
        this.sendMembershipView(reinitializeMessage.getId(), reinitializeMessage.getPort());
    }
}
