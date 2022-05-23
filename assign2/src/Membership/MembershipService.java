package Membership;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MembershipService extends Remote {
    void join() throws RemoteException;
    void leave() throws RemoteException;
}
