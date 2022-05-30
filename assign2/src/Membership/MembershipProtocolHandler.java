package Membership;

import Storage.MembershipLog;
import Storage.MembershipLogEntry;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;

public class MembershipProtocolHandler implements Runnable {
    private final String receivedMessage;
    private final MembershipView membershipView;

    public MembershipProtocolHandler(String receivedMessage, MembershipView membershipView) {
        this.receivedMessage = receivedMessage;
        this.membershipView = membershipView;
    }

    @Override
    public void run() {
        System.out.println("Gonna parse it");
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

        System.out.println("Parsed it");
        if (parsedMessage instanceof MembershipMessageProtocol.JoinMessage joinMessage) {
            System.out.println("received join message on port "
                    + joinMessage.getPort() + " with counter " + joinMessage.getMembershipCounter());
            handleJoin(joinMessage);
        } else if (parsedMessage instanceof MembershipMessageProtocol.LeaveMessage leaveMessage) {
            System.out.println("received leave message with counter " + leaveMessage.getMembershipCounter());
            handleLeave(leaveMessage);
        } else if (parsedMessage instanceof MembershipMessageProtocol.MembershipMessage membershipMessage) {
            System.out.println("received membership message");
            handleMembership(membershipMessage);
        }
    }

    private void handleJoin(MembershipMessageProtocol.JoinMessage joinMessage) {
        membershipView.updateMember(joinMessage.getId(), joinMessage.getMembershipCounter());
        // TODO
    }

    private void handleLeave(MembershipMessageProtocol.LeaveMessage leaveMessage) {
        membershipView.updateMember(leaveMessage.getId(), leaveMessage.getMembershipCounter());
        // TODO
    }

    private void handleMembership(MembershipMessageProtocol.MembershipMessage membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }
}
