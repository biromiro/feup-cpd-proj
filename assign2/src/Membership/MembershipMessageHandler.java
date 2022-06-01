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
                System.out.println("Failed to read from socket");
                exc.printStackTrace();
            }
        });
    }

    private void handleMessage(String message) {
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(message);
        } catch (MessageProtocolException e) {
            System.out.println("Invalid message");
            e.printStackTrace();
            return;
        }

        if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            handleMembership(membershipMessage);
        } else {
            System.out.println("Unexpected message " + parsedMessage);
        }
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }
}
