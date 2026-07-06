import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entry point for every VM in the cloud. Invoked with exactly three arguments:
 * cloud_ip, cloud_port, and VM id.
 *
 * VM 1 (master): Instantiates MasterServiceImpl and runs master.run(), which
 * boots initial VMs, starts the scaling loop, registers as front-end after
 * enough app-tier VMs are ready, and accepts client connections.
 *
 * Other VMs: Look up MasterService in the RMI registry (with unbounded retry
 * until the master is available), call getRole(vmId) to learn FRONT or APP, then
 * either runFrontEnd() or runAppTier(). runFrontEnd registers a
 * FrontEndWorkerServiceImpl, accepts connections from the load balancer, forwards
 * requests to app-tier VMs via round-robin, and reports load. runAppTier
 * registers an AppTierServiceImpl and blocks to receive RMI calls.
 */
public class Server {
    private static final int EXPECTED_ARGS = 3;
    private static final int MASTER_VM_ID = 1;
    /** Max queue depth before load shedding (drop tail). Tune empirically. */
    private static final int QUEUE_THRESHOLD = 6;
    private static final long MASTER_LOOKUP_RETRY_MS = 500;
    private static final long APP_TIER_REFRESH_INTERVAL_MS = 2000;
    private static final int LOAD_REPORT_INTERVAL = 5;
    private static final long APP_TIER_IDLE_SLEEP_MS = 1000;
    /**
     * Entry point for every VM. VM 1 becomes the master; all others contact the master
     * to learn their role (FRONT or APP) and run the appropriate loop.
     *
     * @param args Must have exactly EXPECTED_ARGS elements: [0]=cloud_ip, [1]=cloud_port, [2]=VM id
     * @throws Exception if args.length != EXPECTED_ARGS or if port/VM id are not parseable
     */
    public static void main(String args[]) throws Exception {
        if (args.length != EXPECTED_ARGS)
            throw new Exception("Need " + EXPECTED_ARGS + " args: <cloud_ip> <cloud_port> <VM id>");

        // Parse command-line arguments
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        int vmId = Integer.parseInt(args[2]);

        // Initialize ServerLib for cloud/load-balancer operations
        ServerLib SL = new ServerLib(ip, port);

        // VM 1 runs as master: manages role assignment and RMI registry
        if (vmId == MASTER_VM_ID) {
            MasterServiceImpl master = new MasterServiceImpl(SL, ip, port);
            master.run();
        } else {
            // Non-master VMs: connect to RMI registry and query master for their role
            Registry registry = LocateRegistry.getRegistry(ip, port);

            // Retry lookup until master is available (unbounded -- a transient
            // startup race must not permanently kill this VM)
            MasterService master = null;
            while (master == null) {
                try {
                    master = (MasterService) registry.lookup("MasterService");
                } catch (Exception e) {
                    try {
                        Thread.sleep(MASTER_LOOKUP_RETRY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            String role = master.getRole(vmId);

            if (role.equals("FRONT")) {
                runFrontEnd(SL, ip, port, vmId, master);
            } else {
                runAppTier(SL, ip, port, vmId, master);
            }
        }
    }

    /**
     * Runs the front-end VM loop: register with load balancer, accept connections,
     * parse requests, and forward to app-tier via RMI. Implemented in Step 3.
     *
     * @param SL     ServerLib for cloud/load-balancer operations
     * @param ip     Cloud RMI registry IP (for lookup of app-tier services)
     * @param port   Cloud RMI registry port
     * @param vmId   This VM's ID (for registering FrontEndWorker_RMI stub)
     * @param master MasterService stub for getAppTierNames() and reportLoad()
     */
    static void runFrontEnd(ServerLib SL, String ip, int port,
                            int vmId, MasterService master) {
        try {
            Registry registry = LocateRegistry.getRegistry(ip, port);

            // Register RMI stub so master can signal graceful shutdown
            FrontEndWorkerServiceImpl feService = new FrontEndWorkerServiceImpl(SL);
            registry.rebind("FrontEndWorker_" + vmId, feService);

            SL.registerFrontend();
            master.registerFrontReady(vmId);

            // Cache app-tier names and stubs for round-robin; refresh periodically
            String[] appTierNames = {};
            ConcurrentHashMap<String, AppTierService> feStubCache = new ConcurrentHashMap<>();
            int rrIndex = 0;
            long lastRefresh = 0;
            int iterCount = 0;

            while (!feService.isShuttingDown()) {
                iterCount++;
                // Refresh app-tier list and stub cache periodically
                long now = System.currentTimeMillis();
                if (now - lastRefresh > APP_TIER_REFRESH_INTERVAL_MS) {
                    try {
                        appTierNames = master.getAppTierNames();
                        // Pre-warm stubs for new names
                        for (String name : appTierNames) {
                            if (!feStubCache.containsKey(name)) {
                                try {
                                    feStubCache.put(name, (AppTierService) registry.lookup(name));
                                } catch (Exception e) { }
                            }
                        }
                        // Remove stubs for names no longer in the list
                        feStubCache.keySet().retainAll(Arrays.asList(appTierNames));
                    } catch (Exception e) { /* master gone */ }
                    lastRefresh = now;
                }

                // Report load to master periodically (Fix 7: reduce RMI overhead)
                if (iterCount % LOAD_REPORT_INTERVAL == 0) {
                    try {
                        master.reportLoad(vmId, SL.getQueueLength());
                    } catch (Exception e) { /* master gone at end of sim */ }
                }

                // Load shedding (Hint 8)
                while (SL.getQueueLength() > QUEUE_THRESHOLD) {
                    SL.dropTail();
                }

                ServerLib.Handle h = SL.acceptConnection();
                if (h == null) break; // queue drained after unregister

                Cloud.FrontEndOps.Request r = SL.parseRequest(h);

                // Forward to app tier via round-robin (use cached stubs, Fix 5)
                boolean forwarded = false;
                if (appTierNames.length > 0) {
                    for (int attempt = 0; attempt < appTierNames.length; attempt++) {
                        String name = appTierNames[rrIndex % appTierNames.length];
                        rrIndex++;
                        AppTierService appTier = feStubCache.get(name);
                        if (appTier == null) {
                            try {
                                appTier = (AppTierService) registry.lookup(name);
                                if (appTier != null) feStubCache.put(name, appTier);
                            } catch (Exception e) { continue; }
                        }
                        if (appTier == null) continue;
                        try {
                            appTier.processRequest(r);
                            forwarded = true;
                            break;
                        } catch (Exception e) {
                            feStubCache.remove(name);  // Invalidate on failure
                        }
                    }
                }
                if (!forwarded) {
                    SL.dropRequest(r);
                }
            }

            // Drain phase: after unregister, handle remaining queued connections
            while (SL.getQueueLength() > 0) {
                ServerLib.Handle h = SL.acceptConnection();
                if (h == null) break;
                Cloud.FrontEndOps.Request r = SL.parseRequest(h);
                boolean forwarded = false;
                if (appTierNames.length > 0) {
                    for (int attempt = 0; attempt < appTierNames.length; attempt++) {
                        String name = appTierNames[rrIndex % appTierNames.length];
                        rrIndex++;
                        AppTierService appTier = feStubCache.get(name);
                        if (appTier == null) {
                            try {
                                appTier = (AppTierService) registry.lookup(name);
                                if (appTier != null) feStubCache.put(name, appTier);
                            } catch (Exception e) { continue; }
                        }
                        if (appTier == null) continue;
                        try {
                            appTier.processRequest(r);
                            forwarded = true;
                            break;
                        } catch (Exception e) {
                            feStubCache.remove(name);
                        }
                    }
                }
                if (!forwarded) SL.dropRequest(r);
            }

            // Unexport so the VM process can exit
            feService.unexport();

        } catch (Exception e) {
            // End of simulation
        }
    }

    /**
     * Runs the app-tier VM: register AppTierService in RMI registry, notify
     * master of readiness, then block to receive RMI calls.
     *
     * @param SL     ServerLib for processRequest()
     * @param ip     Cloud RMI registry IP
     * @param port   Cloud RMI registry port
     * @param vmId   This VM's ID (used for RMI name "AppTierService_" + vmId)
     * @param master MasterService stub (passed from main to avoid redundant lookup)
     */
    static void runAppTier(ServerLib SL, String ip, int port, int vmId,
                           MasterService master) {
        try {
            Registry registry = LocateRegistry.getRegistry(ip, port);
            AppTierServiceImpl impl = new AppTierServiceImpl(SL);

            String name = "AppTierService_" + vmId;
            registry.rebind(name, impl);

            master.registerAppReady(vmId);

            while (true) {
                Thread.sleep(APP_TIER_IDLE_SLEEP_MS);
            }
        } catch (Exception e) {
            // End of simulation or master gone
        }
    }
}
