package Membership;

import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

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
        if (parsedMessage instanceof MembershipMessageProtocol.Join joinMessage) {
            System.out.println("received join message on port "
                    + joinMessage.getPort() + " with counter " + joinMessage.getMembershipCounter());
            handleJoin(joinMessage);
        } else if (parsedMessage instanceof MembershipMessageProtocol.Leave leaveMessage) {
            System.out.println("received leave message with counter " + leaveMessage.getMembershipCounter());
            handleLeave(leaveMessage);
        } else if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            System.out.println("received membership message");
            handleMembership(membershipMessage);
        } else if (parsedMessage instanceof MembershipMessageProtocol.Reinitialize reinitializeMessage) {
            System.out.println("received reinitialize message on port"
            + reinitializeMessage.getPort());
            handleReinitialize(reinitializeMessage);
        }
    }

    private void handleReinitialize(MembershipMessageProtocol.Reinitialize reinitializeMessage) {
        // TODO maybe should send the membership counter as well on reinitialize message?
        membershipView.setPriority(membershipView.getPriority() - 1);
    }

    private void handleJoin(MembershipMessageProtocol.Join joinMessage) {
        membershipView.updateMember(joinMessage.getId(), joinMessage.getMembershipCounter());
        membershipView.setPriority(membershipView.getPriority() + 1);
    }

    private void handleLeave(MembershipMessageProtocol.Leave leaveMessage) {
        membershipView.updateMember(leaveMessage.getId(), leaveMessage.getMembershipCounter());
        // TODO
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }
}
