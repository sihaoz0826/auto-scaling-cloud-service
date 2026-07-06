import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Front-end worker service implementation. Each non-master front-end VM exports
 * this as "FrontEndWorker_<vmId>" so the master can signal shutdown.
 *
 * shutdownGracefully() is called by the master during scale-down. It sets
 * shuttingDown to true and unregisters from the load balancer
 * (SL.unregisterFrontend). The runFrontEnd loop in Server checks isShuttingDown()
 * each iteration and exits when true, then drains remaining queued connections
 * before calling unexport() so the VM process can exit cleanly.
 */
public class FrontEndWorkerServiceImpl extends UnicastRemoteObject
        implements FrontEndWorkerService {
    private ServerLib SL;
    private volatile boolean shuttingDown = false;

    /**
     * Creates the front-end worker service.
     *
     * @param SL ServerLib for cloud/load-balancer operations
     */
    public FrontEndWorkerServiceImpl(ServerLib SL) throws Exception {
        super();
        this.SL = SL;
    }

    /**
     * Initiates graceful shutdown: sets flag and unregisters from load balancer.
     *
     * @throws RemoteException on RMI failure
     */
    @Override
    public void shutdownGracefully() throws RemoteException {
        shuttingDown = true;
        SL.unregisterFrontend();
    }

    /**
     * Returns whether this worker has been asked to shut down.
     *
     * @return true if shutdownGracefully() was called, false otherwise
     */
    public boolean isShuttingDown() {
        return shuttingDown;
    }

    /**
     * Unexports this RMI object so the VM process can exit.
     */
    public void unexport() {
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (Exception e) {
        }
    }
}
