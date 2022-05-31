package Membership;

import Connection.AsyncTcpConnection;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

public class MembershipMessageHandler implements Runnable {
    private final AsyncTcpConnection worker;
    private final MembershipView membershipView;

    MembershipMessageHandler(AsyncTcpConnection worker, MembershipView membershipView) {
        this.worker = worker;
        this.membershipView = membershipView;
    }

    @Override
    public void run() {
        worker.read(new AsyncTcpConnection.ReadHandler() {
            @Override
            public void completed(Integer result, String message) {
                handleMessage(message);
            }

            @Override
            public void failed(Throwable exc) {
                throw new RuntimeException("Failed to read from socket", exc);
            }
        });
    }

    private void handleMessage(String message) {
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(message);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

        if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            handleMembership(membershipMessage);
        } else {
            throw new RuntimeException("Unexpected message " + parsedMessage);
        }
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }
}
