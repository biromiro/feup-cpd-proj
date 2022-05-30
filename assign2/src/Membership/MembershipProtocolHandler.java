package Membership;

import Message.MembershipLog;
import Message.MembershipLogEntry;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.io.IOException;

public class MembershipProtocolHandler implements Runnable {
    private final String receivedMessage;
    private final MembershipLog membershipLog;

    public MembershipProtocolHandler(String receivedMessage, MembershipLog membershipLog) {
        this.receivedMessage = receivedMessage;
        this.membershipLog = membershipLog;
    }

    @Override
    public void run() {
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(receivedMessage);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

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
        try {
            membershipLog.log(new MembershipLogEntry(joinMessage.getId(), joinMessage.getMembershipCounter()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // TODO
    }

    private void handleLeave(MembershipMessageProtocol.LeaveMessage leaveMessage) {
        try {
            membershipLog.log(new MembershipLogEntry(leaveMessage.getId(), leaveMessage.getMembershipCounter()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // TODO
    }

    private void handleMembership(MembershipMessageProtocol.MembershipMessage membershipMessage) {
        // TODO
    }
}
