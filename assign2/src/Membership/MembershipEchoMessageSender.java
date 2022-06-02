package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MembershipEchoMessageSender implements Runnable {
    private final MulticastConnection clusterConnection;
    private final MembershipView membershipView;

    public MembershipEchoMessageSender(MulticastConnection clusterConnection, MembershipView membershipView) {
        this.clusterConnection = clusterConnection;
        this.membershipView = membershipView;
    }

    @Override
    public void run() {
        if (membershipView.mayMulticast()) {
            membershipView.startMulticasting();
            try {
                clusterConnection.send(MembershipMessageProtocol.membershipLog(membershipView));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
