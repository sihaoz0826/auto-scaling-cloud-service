import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI interface for the master service. Exported as "MasterService" in the
 * registry. Implemented by MasterServiceImpl on VM 1.
 *
 * New VMs use getRole(vmId) to learn their assigned role (FRONT or APP) before
 * starting their service loops. Front-end VMs call reportLoad(vmId, queueLength)
 * periodically so the master can compute average queue depth for scaling.
 * Front-ends use getAppTierNames() to discover active app-tier RMI names for
 * round-robin forwarding. App-tier and front-end VMs call registerAppReady(vmId)
 * / registerFrontReady(vmId) once they have bound their stubs and are ready to
 * accept traffic.
 */
public interface MasterService extends Remote {
    /**
     * Called by each new VM on boot to learn its assigned role.
     *
     * @param vmId The VM's ID
     * @return "FRONT" or "APP"
     * @throws RemoteException on RMI failure
     */
    String getRole(int vmId) throws RemoteException;

    /**
     * Front-ends report their queue length periodically for scaling decisions.
     *
     * @param vmId        The front-end VM's ID
     * @param queueLength Number of connections waiting
     * @throws RemoteException on RMI failure
     */
    void reportLoad(int vmId, int queueLength) throws RemoteException;

    /**
     * Returns RMI registry names of active app-tier VMs for round-robin forwarding.
     *
     * @return Array of names (e.g. "AppTierService_2")
     * @throws RemoteException on RMI failure
     */
    String[] getAppTierNames() throws RemoteException;

    /**
     * Called by an app-tier VM after it has bound its RMI service and is ready
     * to receive requests.
     *
     * @param vmId The app-tier VM's ID
     * @throws RemoteException on RMI failure
     */
    void registerAppReady(int vmId) throws RemoteException;

    /**
     * Called by a front-end VM after it has bound its RMI service and registered
     * with the load balancer.
     *
     * @param vmId The front-end VM's ID
     * @throws RemoteException on RMI failure
     */
    void registerFrontReady(int vmId) throws RemoteException;
}
