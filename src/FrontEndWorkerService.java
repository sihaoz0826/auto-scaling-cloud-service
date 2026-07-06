import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI interface for front-end worker VMs. Exported as "FrontEndWorker_<vmId>" in
 * the registry. Implemented by FrontEndWorkerServiceImpl.
 *
 * The master is the only caller. When scaling down front-end capacity, the
 * master looks up this stub and calls shutdownGracefully(). The front-end should
 * then unregister from the load balancer, drain any queued connections, and
 * exit. This allows the master to reduce front-end VMs without leaving orphaned
 * processes or breaking the load balancer.
 */
public interface FrontEndWorkerService extends Remote {
    /**
     * Master calls this to initiate graceful shutdown. The front-end should
     * unregister from load balancer, drain remaining connections, then exit.
     *
     * @throws RemoteException on RMI failure
     */
    void shutdownGracefully() throws RemoteException;
}
