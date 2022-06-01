package Membership;

import Connection.AsyncTcpConnection;
import Message.MembershipMessageProtocol;
import Message.MessageProtocolException;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class MembershipMessageHandler implements Runnable {
    private final AsyncTcpConnection worker;
    private final MembershipView membershipView;
    private final Map<String, Integer> blacklist;

    MembershipMessageHandler(AsyncTcpConnection worker, MembershipView membershipView, Map<String, Integer> blacklist) {
        this.worker = worker;
        this.membershipView = membershipView;
        this.blacklist = blacklist;
    }

    @Override
    public void run() {
        worker.read(new AsyncTcpConnection.ReadHandler() {
            @Override
            public void completed(Integer result, String message) {
                MembershipMessageProtocol.Membership membershipMessage = handleMessage(message);
                blacklist.put(membershipMessage.getId(), membershipView.getCount());
            }

            @Override
            public void failed(Throwable exc) {
                System.out.println("Failed to read from socket");
                exc.printStackTrace();
            }
        });
    }

    private MembershipMessageProtocol.Membership handleMessage(String message) {
        MembershipMessageProtocol parsedMessage;
        try {
            parsedMessage = MembershipMessageProtocol.parse(message);
        } catch (MessageProtocolException e) {
            throw new RuntimeException("Invalid message", e);
        }

        if (parsedMessage instanceof MembershipMessageProtocol.Membership membershipMessage) {
            handleMembership(membershipMessage);
            return membershipMessage;
        } else {
            throw new RuntimeException("Unexpected message type");
        }
    }

    private void handleMembership(MembershipMessageProtocol.Membership membershipMessage) {
        membershipView.merge(membershipMessage.getMembers(), membershipMessage.getLog());
        // TODO
    }
}
