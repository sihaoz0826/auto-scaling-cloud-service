import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI interface for app-tier VMs. Exported as "AppTierService_<vmId>" in the
 * registry. Implemented by AppTierServiceImpl.
 *
 * Front-ends call processRequest(r) to forward parsed client requests; the call
 * returns immediately (fire-and-forget) while work is queued. If the app-tier
 * queue is full, processRequest throws RemoteException so the caller can retry
 * another VM. The master calls getPendingCount() to aggregate load across
 * app-tier VMs for scale-up/scale-down decisions. shutdown() is invoked by the
 * master when scaling down to drain the thread pool and unexport before
 * terminating the VM.
 */
public interface AppTierService extends Remote {
    /**
     * Queues the parsed request for processing. Returns immediately (fire-and-forget).
     *
     * @param r The parsed client request to process
     * @throws RemoteException on RMI failure
     */
    void processRequest(Cloud.FrontEndOps.Request r) throws RemoteException;

    /**
     * Gracefully shuts down: stop accepting new work, drain thread pool, unexport.
     *
     * @throws RemoteException on RMI failure
     */
    void shutdown() throws RemoteException;

    /**
     * Returns the number of requests currently pending (submitted but not completed).
     * Used by the master for load-based scaling decisions.
     *
     * @return count of pending requests
     * @throws RemoteException on RMI failure
     */
    int getPendingCount() throws RemoteException;
}
