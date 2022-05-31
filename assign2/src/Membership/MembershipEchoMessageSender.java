package Membership;

import Connection.MulticastConnection;
import Message.MembershipMessageProtocol;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MembershipEchoMessageSender implements Runnable {
    private static final int TIMEOUT = 1000;
    private final MulticastConnection clusterConnection;
    private final MembershipView membershipView;
    private final ThreadPoolExecutor executor;

    public MembershipEchoMessageSender(ThreadPoolExecutor executor, MulticastConnection clusterConnection, MembershipView membershipView) {
        this.executor = executor;
        this.clusterConnection = clusterConnection;
        this.membershipView = membershipView;
    }
    @Override
    public void run() {
        if (membershipView.isBroadcasting()) {
            try {
                clusterConnection.send(MembershipMessageProtocol.membershipLog(membershipView));
                // TODO isto cria um novo executor. Nos so deviamos usar o executor original
                CompletableFuture.delayedExecutor(TIMEOUT, TimeUnit.MILLISECONDS).execute(() -> {
                    executor.submit(new MembershipEchoMessageSender(executor, clusterConnection, membershipView));
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
