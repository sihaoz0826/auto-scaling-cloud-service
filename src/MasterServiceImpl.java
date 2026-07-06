import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master service implementation. Runs exclusively on VM 1 and serves as the
 * central orchestrator for the cloud.
 *
 * Role assignment: New VMs call getRole(vmId) on boot to learn whether they
 * are FRONT or APP; roles are assigned before VM start via startVMWithRole()
 * to avoid FIFO races.
 *
 * VM registration: App-tier and front-end VMs call registerAppReady /
 * registerFrontReady once their RMI stubs are bound and they are ready to
 * serve traffic.
 *
 * Load tracking: Front-ends periodically report queue length via reportLoad();
 * app-tier pending counts are queried via getPendingCount().
 *
 * Scaling: A background scalingLoop polls every 500ms, scales up when
 * avgAppPending or avgQueue exceed thresholds, and scales down when both are
 * low for SCALE_DOWN_PATIENCE_MS. Uses cooldowns to avoid thrashing.
 *
 * Request forwarding: The master also acts as a front-end: it accepts
 * connections, performs load shedding (drop tail), and forwards requests to
 * app-tier VMs via round-robin with retry (forwardWithRetry).
 *
 * Graceful shutdown: scaleDownAppTier/scaleDownFrontEnd shut down the
 * least-loaded VM and clean up stub caches.
 */
public class MasterServiceImpl extends UnicastRemoteObject implements MasterService {
    private ServerLib SL;
    private String ip;
    private int port;

    private Set<Integer> frontEndVMs = ConcurrentHashMap.newKeySet();
    private Set<Integer> appTierVMs = ConcurrentHashMap.newKeySet();

    // Explicit VM ID to role mapping (avoids FIFO race condition)
    private ConcurrentHashMap<Integer, String> assignedRoles = new ConcurrentHashMap<>();

    // Track VMs that are booting, with boot start time for timeout detection
    private ConcurrentHashMap<Integer, Long> bootingVMs = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, Integer> frontEndLoads = new ConcurrentHashMap<>();

    // Round-robin counter for forwardWithRetry
    private AtomicInteger rrCounter = new AtomicInteger(0);

    // Cache RMI stubs for app-tier VMs (Fix 5)
    private ConcurrentHashMap<Integer, AppTierService> appTierStubs = new ConcurrentHashMap<>();

    private static final int INITIAL_FRONT = 0;  // master alone is enough
    private static final int INITIAL_APP = 5;    // R2-D
    private static final int REGISTER_THRESHOLD = 5;  // R2-D
    private static final int QUEUE_THRESHOLD = 6;
    private static final long BOOT_TIMEOUT_MS = 20000;

    // --- Tuning knobs (see design_docs/project3_checkpoint2_revision4_plan.md) ---
    private static final int APP_PENDING_SCALE_UP_THRESHOLD = 2;  // UP_PEND
    private static final int SCALE_UP_QUEUE_THRESHOLD = 4;        // UP_QUEUE
    private static final int SCALE_DOWN_QUEUE_THRESHOLD = 1;
    private static final long SCALE_UP_COOLDOWN_MS = 1500;        // R1-A
    private static final long SCALE_DOWN_COOLDOWN_MS = 2000;      // DOWN_COOL
    private static final long SCALE_DOWN_PATIENCE_MS = 5000;     // DOWN_PAT
    private static final int BOOTSTRAP_QUEUE_DIVISOR = 8;         // R1-A
    private static final int MIN_FRONT = 0;
    private static final int MIN_APP = 1;
    private static final int MAX_CONCURRENT_BOOTS = 5;            // R2-D
    private static final int MAX_APP = 6;
    private static final int MASTER_VM_ID = 1;
    private static final long REGISTRATION_POLL_MS = 100;
    private static final long SCALING_LOOP_INTERVAL_MS = 500;
    private static final int SCALE_UP_BATCH_SIZE = 1;
    private static final int FRONT_TO_APP_RATIO = 3;
    private static final double SCALE_DOWN_PENDING_THRESHOLD = 1.0;
    private static final int MASTER_FRONT_COUNT = 1;  // master also acts as a front-end

    /**
     * Creates the master service. Called by Server when booting the master VM.
     *
     * @param SL   ServerLib instance for VM operations
     * @param ip   RMI registry host
     * @param port RMI registry port
     */
    public MasterServiceImpl(ServerLib SL, String ip, int port) throws Exception {
        super();
        this.SL = SL;
        this.ip = ip;
        this.port = port;
    }

    /**
     * Returns the role (FRONT or APP) for a newly booted VM. Side-effect free:
     * does NOT add the VM to any active set or remove it from bootingVMs.
     * The VM must call registerAppReady() or registerFrontReady() once it is
     * truly ready to serve.
     *
     * @param vmId VM identifier from startVM()
     * @return "FRONT" or "APP"
     */
    @Override
    public String getRole(int vmId) throws RemoteException {
        return assignedRoles.getOrDefault(vmId, "APP");
    }

    /**
     * Called by an app-tier VM after it has bound its AppTierService in the RMI
     * registry. Adds the VM to appTierVMs and removes it from bootingVMs.
     *
     * @param vmId The app-tier VM's ID
     * @throws RemoteException on RMI failure
     */
    @Override
    public void registerAppReady(int vmId) throws RemoteException {
        appTierVMs.add(vmId);
        bootingVMs.remove(vmId);
    }

    /**
     * Called by a front-end VM after it has bound its FrontEndWorkerService and
     * registered with the load balancer. Adds the VM to frontEndVMs and removes
     * it from bootingVMs.
     *
     * @param vmId The front-end VM's ID
     * @throws RemoteException on RMI failure
     */
    @Override
    public void registerFrontReady(int vmId) throws RemoteException {
        frontEndVMs.add(vmId);
        bootingVMs.remove(vmId);
    }

    /**
     * Records front-end queue length for a VM. Called periodically by front-end
     * workers. Used by the scaling loop to compute avgQueue.
     *
     * @param vmId        VM identifier
     * @param queueLength Current queue depth at that front-end
     * @return No return value
     */
    @Override
    public void reportLoad(int vmId, int queueLength) throws RemoteException {
        frontEndLoads.put(vmId, queueLength);
    }

    /**
     * Returns RMI stub names for all registered app-tier VMs. Used by front-ends
     * to discover app-tier services for load balancing.
     *
     * @return array of "AppTierService_N" strings
     */
    @Override
    public String[] getAppTierNames() throws RemoteException {
        List<String> names = new ArrayList<>();
        for (int id : appTierVMs) {
            names.add("AppTierService_" + id);
        }
        return names.toArray(new String[0]);
    }

    /**
     * Starts a new VM and assigns it a role. Records the VM in bootingVMs until
     * it registers via getRole().
     *
     * @param role "FRONT" or "APP"
     * @return No return value
     */
    private void startVMWithRole(String role) {
        int id = SL.startVM();
        assignedRoles.put(id, role);
        bootingVMs.put(id, System.currentTimeMillis());
    }

    /**
     * Returns a cached app-tier stub for the given VM ID. Refreshes from
     * registry on cache miss; invalidates cache on exception (Fix 5).
     *
     * @param id app-tier VM ID
     * @return stub, or null on lookup failure
     */
    private AppTierService getAppTierStub(int id) {
        AppTierService stub = appTierStubs.get(id);
        if (stub != null) return stub;
        try {
            Registry registry = LocateRegistry.getRegistry(ip, port);
            stub = (AppTierService) registry.lookup("AppTierService_" + id);
            appTierStubs.put(id, stub);
            return stub;
        } catch (Exception e) {
            appTierStubs.remove(id);
            return null;
        }
    }

    /**
     * Forwards a request to an app-tier VM with round-robin retry. Handles all
     * exceptions internally (queue full, VM died) so the accept loop never crashes.
     * Invalidates stubs on failure and tries the next VM (Fix 8).
     *
     * @param r the parsed client request to forward
     * @return true if forwarded successfully, false if all VMs failed
     */
    private boolean forwardWithRetry(Cloud.FrontEndOps.Request r) {
        List<Integer> ids = new ArrayList<>(appTierVMs);
        if (ids.isEmpty()) return false;

        int size = ids.size();
        for (int attempt = 0; attempt < size; attempt++) {
            int idx = Math.abs(rrCounter.getAndIncrement()) % size;
            int id = ids.get(idx);
            try {
                AppTierService stub = getAppTierStub(id);
                if (stub == null) continue;
                stub.processRequest(r);
                return true;
            } catch (RemoteException e) {
                // Queue full (Fix 6) or VM died -- invalidate cache, try next
                appTierStubs.remove(id);
            } catch (Exception e) {
                appTierStubs.remove(id);
                appTierVMs.remove(id);
            }
        }
        return false;
    }

    /**
     * Sums pending request counts across all app-tier VMs. Used by the scaling
     * loop as the primary scale-up signal.
     *
     * @return total pending requests, or 0 on registry failure
     */
    private int getTotalAppTierPending() {
        int total = 0;
        for (int id : appTierVMs) {
            try {
                AppTierService svc = getAppTierStub(id);
                if (svc != null) total += svc.getPendingCount();
            } catch (Exception e) {
                appTierStubs.remove(id);
            }
        }
        return total;
    }

    /**
     * Main master loop. Boots VMs first, waits for REGISTER_THRESHOLD app-tier
     * VMs, then registers as frontend. Delayed registration avoids cascade
     * of stale connections when VMs come online (R1-E design).
     *
     * @return No return value (blocks until shutdown)
     */
    public void run() {
        try {
            Registry registry = LocateRegistry.getRegistry(ip, port);
            registry.rebind("MasterService", this);

            // 1. Boot initial app-tier VMs eagerly
            for (int i = 0; i < INITIAL_APP; i++) startVMWithRole("APP");

            // 2. Start scaling loop
            new Thread(() -> scalingLoop()).start();

            // 3. Wait for enough app VMs before accepting traffic (delayed reg)
            while (appTierVMs.size() < REGISTER_THRESHOLD) {
                Thread.sleep(REGISTRATION_POLL_MS);
            }

            // 4. NOW register as frontend -- VMs are ready, no backlog
            SL.registerFrontend();
            frontEndVMs.add(MASTER_VM_ID);

            // 5. Boot initial front-end workers
            for (int i = 0; i < INITIAL_FRONT; i++) startVMWithRole("FRONT");

            // 6. Master's accept loop
            while (true) {
                frontEndLoads.put(MASTER_VM_ID, SL.getQueueLength());

                while (SL.getQueueLength() > QUEUE_THRESHOLD) {
                    SL.dropTail();
                }

                ServerLib.Handle h = SL.acceptConnection();
                if (h == null) break;
                Cloud.FrontEndOps.Request r = SL.parseRequest(h);

                if (!forwardWithRetry(r)) {
                    SL.dropRequest(r);
                }
            }
        } catch (Exception e) {
            // End of simulation
        }
    }

    /**
     * Background thread that scales VMs up and down. Polls every SCALING_LOOP_INTERVAL_MS. Scale-up
     * uses avgAppPending and avgQueue; scale-down requires both to be low for
     * SCALE_DOWN_PATIENCE_MS before removing a VM.
     *
     * @return No return value (runs until interrupted)
     */
    private void scalingLoop() {
        long lastScaleUp = 0;
        long lastScaleDown = 0;
        long lowLoadSince = 0;

        while (true) {
            try {
                Thread.sleep(SCALING_LOOP_INTERVAL_MS);
                long now = System.currentTimeMillis();

                // Clean up stale booting VMs
                for (Map.Entry<Integer, Long> entry : bootingVMs.entrySet()) {
                    if (now - entry.getValue() > BOOT_TIMEOUT_MS) {
                        bootingVMs.remove(entry.getKey());
                        assignedRoles.remove(entry.getKey());
                    }
                }

                int activeApps = appTierVMs.size();
                int activeFronts = frontEndVMs.size();
                int booting = bootingVMs.size();

                // Compute front-end queue average
                double avgQueue = 0;
                int feCount = 0;
                for (int q : frontEndLoads.values()) {
                    avgQueue += q;
                    feCount++;
                }
                if (feCount > 0) avgQueue /= feCount;

                // Compute total app-tier pending work
                int totalAppPending = getTotalAppTierPending();
                double avgAppPending = activeApps > 0 ?
                    (double) totalAppPending / activeApps : 0;

                // --- SCALE UP ---
                if (now - lastScaleUp >= SCALE_UP_COOLDOWN_MS
                        && booting < MAX_CONCURRENT_BOOTS) {

                    boolean needMoreApps = avgAppPending > APP_PENDING_SCALE_UP_THRESHOLD;
                    boolean needMoreFronts = avgQueue > SCALE_UP_QUEUE_THRESHOLD;

                    if ((needMoreApps || needMoreFronts) && activeApps < MAX_APP) {
                        int toAdd = Math.min(SCALE_UP_BATCH_SIZE, MAX_CONCURRENT_BOOTS - booting);
                        for (int i = 0; i < toAdd; i++) {
                            // Favor app-tier (the bottleneck) unless fronts are
                            // genuinely backed up and ratio is already app-heavy
                            if (needMoreApps || activeFronts * FRONT_TO_APP_RATIO > activeApps) {
                                startVMWithRole("APP");
                                activeApps++;
                            } else {
                                startVMWithRole("FRONT");
                                activeFronts++;
                            }
                        }
                        lastScaleUp = now;
                        lowLoadSince = 0;
                    }
                }

                // --- SCALE DOWN (conservative) ---
                if (now - lastScaleDown >= SCALE_DOWN_COOLDOWN_MS) {
                    boolean loadIsLow = avgQueue < SCALE_DOWN_QUEUE_THRESHOLD
                                     && avgAppPending < SCALE_DOWN_PENDING_THRESHOLD;

                    if (loadIsLow) {
                        if (lowLoadSince == 0) {
                            lowLoadSince = now;
                        } else if (now - lowLoadSince > SCALE_DOWN_PATIENCE_MS) {
                            if (activeApps > MIN_APP) {
                                scaleDownAppTier();
                            } else if (activeFronts > MIN_FRONT + MASTER_FRONT_COUNT) {
                                scaleDownFrontEnd();
                            }
                            lastScaleDown = now;
                            lowLoadSince = 0;
                        }
                    } else {
                        lowLoadSince = 0;
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                break; // end of simulation
            }
        }
    }

    /**
     * Shuts down one app-tier VM gracefully (drain, unexport) and removes it
     * from appTierVMs. Picks the least-loaded victim via getPendingCount()
     * to minimize disruption. Cleans up stub cache (Fix 8).
     *
     * @return No return value
     */
    private void scaleDownAppTier() {
        // Pick the least-loaded victim
        Integer victim = null;
        int minPending = Integer.MAX_VALUE;

        for (int id : appTierVMs) {
            try {
                AppTierService svc = getAppTierStub(id);
                if (svc == null) {
                    // Unreachable -- good removal candidate
                    victim = id;
                    break;
                }
                int pending = svc.getPendingCount();
                if (pending < minPending) {
                    minPending = pending;
                    victim = id;
                }
            } catch (Exception e) {
                // Can't reach this VM -- it's a good removal candidate
                victim = id;
                break;
            }
        }
        if (victim == null) return;

        try {
            AppTierService svc = getAppTierStub(victim);
            if (svc != null) {
                svc.shutdown();
            }
        } catch (Exception e) {
            // RMI failed; endVM will force-kill
        }

        appTierVMs.remove(victim);
        appTierStubs.remove(victim);  // Clean up stub cache
        SL.endVM(victim);
    }

    /**
     * Shuts down one front-end VM (not the master) gracefully and removes it
     * from frontEndVMs. Picks an arbitrary non-master victim.
     *
     * @return No return value
     */
    private void scaleDownFrontEnd() {
        Integer victim = null;
        for (int id : frontEndVMs) {
            if (id != MASTER_VM_ID) {
                victim = id;
                break;
            }
        }
        if (victim == null) return;

        try {
            Registry registry = LocateRegistry.getRegistry(ip, port);
            FrontEndWorkerService feSvc = (FrontEndWorkerService)
                    registry.lookup("FrontEndWorker_" + victim);
            feSvc.shutdownGracefully();
        } catch (Exception e) {
            // RMI failed; endVM below will force-kill
        }

        frontEndVMs.remove(victim);
        frontEndLoads.remove(victim);
        SL.endVM(victim);
    }
}
