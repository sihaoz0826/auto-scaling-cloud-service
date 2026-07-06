import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * App-tier service implementation. Each app-tier VM exports this service in the
 * RMI registry as "AppTierService_<vmId>".
 *
 * Request handling: processRequest() queues work in a bounded thread pool
 * (THREAD_POOL_SIZE workers, MAX_QUEUE_DEPTH pending) and returns immediately.
 * If the queue is full, it throws RemoteException so the caller can retry
 * another VM. Actual processing is done by ServerLib.processRequest() on worker
 * threads.
 *
 * The master uses getPendingCount() to compute total app-tier load for scaling
 * decisions. shutdown() is called by the master during scale-down to drain the
 * pool and unexport the RMI object before the VM is terminated.
 */
public class AppTierServiceImpl extends UnicastRemoteObject implements AppTierService {
    private ServerLib SL;
    private ExecutorService executor;
    private AtomicInteger pending = new AtomicInteger(0);

    private static final int THREAD_POOL_SIZE = 2;  // R1-E
    private static final int MAX_QUEUE_DEPTH = 4;  // R2-D
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 3;
    private static final long THREAD_KEEP_ALIVE_MS = 0L;

    /**
     * Creates the app-tier service with a thread pool for request processing.
     *
     * @param SL ServerLib for processRequest()
     */
    public AppTierServiceImpl(ServerLib SL) throws Exception {
        super();
        this.SL = SL;
        this.executor = new ThreadPoolExecutor(
            THREAD_POOL_SIZE, THREAD_POOL_SIZE,
            THREAD_KEEP_ALIVE_MS, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(MAX_QUEUE_DEPTH),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    /**
     * Queues the request for the thread pool; returns immediately.
     *
     * @param r Parsed request from front-end
     * @throws RemoteException on RMI failure
     */
    @Override
    public void processRequest(Cloud.FrontEndOps.Request r) throws RemoteException {
        pending.incrementAndGet();
        try {
            executor.submit(() -> {
                try {
                    SL.processRequest(r);
                } catch (Exception e) { /* end of simulation */ }
                finally {
                    pending.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            pending.decrementAndGet();
            throw new RemoteException("App-tier queue full");
        }
    }

    /**
     * Returns the number of requests currently pending in the queue.
     *
     * @return count of pending (submitted but not completed) requests
     * @throws RemoteException on RMI failure
     */
    @Override
    public int getPendingCount() throws RemoteException {
        return pending.get();
    }

    /**
     * Shuts down the thread pool and unexports this RMI object.
     *
     * @throws RemoteException on RMI failure
     */
    @Override
    public void shutdown() throws RemoteException {
        executor.shutdown();
        try {
            executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) { }
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (Exception e) { }
    }
}
