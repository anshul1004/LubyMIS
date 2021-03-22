import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulates a synchronous distributed system and implements LubyMIS algorithm using multi threading
 * @author Anshul Pardhi, Jayita Roy, Ruchi Singh
 */
public class Luby {

    // Shared memory
    static Map<String, Integer> randomIdsMap = new HashMap<>();
    static Map<String, Boolean> isCandidateMap = new HashMap<>();
    static Map<String, Boolean> isIndependentMap = new HashMap<>();
    static CyclicBarrier gate = null;
    static int rounds = 0;

    /**
     * Simulates the work done by a process node. Here, every process has its own thread
     */
    private static class ProcessNode implements Runnable {

        private final List<Integer> neighbors;
        int roundCount = 0;

        ProcessNode(int i, int n, int pid, List<Integer> neighbors) {

            int randomId = new Random().nextInt((int) Math.pow(n, 4)); // Assign a random id
            randomIdsMap.put(Integer.toString(pid), randomId);

            this.neighbors = neighbors; // Contains ids of neighboring process nodes

            // Initially every process nodes are candidate nodes and none are present in the independent set
            isCandidateMap.put(Integer.toString(pid), true);
            isIndependentMap.put(Integer.toString(pid), false);

            System.out.println("Current process id: " + pid + "; Randomly assigned id: " + randomId);
        }

        /**
         * Luby MIS algorithm implementation
         */
        public void run() {
            try {
                gate.await();
                String currentThread = Thread.currentThread().getName();

                while(isCandidateMap.get(currentThread)) {
                    //Thread.sleep(4000);

                    int currId = randomIdsMap.get(currentThread);
                    boolean isMax = true;
                    for(int neighbor : neighbors) {
                        String neighborThreadName = Integer.toString(neighbor);
                        if (isCandidateMap.get(neighborThreadName) && randomIdsMap.get(neighborThreadName) >= currId) {
                            isMax = false; // Some other node has id greater than the current node
                            break;
                        }
                    }

                    // This is done to maintain a synchronous network
                    Thread.sleep(4000); // Wait for other processes to find leaders in their neighborhoods

                    if(isMax) { // Current node has max id among all its neighbors
                        isIndependentMap.put(currentThread, true); // Put the current node in MIS
                        isCandidateMap.put(currentThread, false);
                        for(int neighbor : neighbors) { // Neighbors won't be in MIS
                            isCandidateMap.put(Integer.toString(neighbor), false);
                        }
                    }
                    System.out.println("Current executing process id: " + currentThread);
                    roundCount++;
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            rounds = Math.max(rounds, roundCount);
        }
    }

    /**
     * Prints the MIS process ids
     * @return Set containing MIS process ids
     */
    private static Set<Integer> printMIS() {

        Set<Integer> misSet = new HashSet<>();
        System.out.print("MIS has the processes with IDs: ");
        for(Map.Entry<String, Boolean> node : isIndependentMap.entrySet()) {
            if(node.getValue()) {
                misSet.add(Integer.parseInt(node.getKey()));
                System.out.print(node.getKey() + " ");
            }
        }

        System.out.println();
        return misSet;
    }

    /**
     * Verifies if the MIS constructed is indeed correct
     * @param n Total number of processes
     * @param ids Ids corresponding to each process
     * @param misSet Set containing MIS process ids
     * @param processNeighbors Contains a list of neighboring process ids for each process
     */
    private static void verifyMIS(int n, int[] ids, Set<Integer> misSet, List<List<Integer>> processNeighbors) {

        System.out.println("Verifying the MIS constructed is correct...");
        for(int i=0; i<n; i++) {
            for(int j=0; j<processNeighbors.get(i).size(); j++) {
                if(misSet.contains(ids[i]) && misSet.contains(processNeighbors.get(i).get(j))) {
                    System.out.println("THE MIS CONSTRUCTED IS NOT CORRECT....EXITING PROGRAM...");
                    System.exit(1);
                }
            }
        }
        System.out.println("Verified that the MIS constructed is indeed correct!");
    }

    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {

        // Pass on input file through command line argument
        if(args.length != 1) {
            System.err.println("Error: Input file not provided in the argument!");
            System.exit(1);
        }

        Scanner sc = null;
        try {
            sc = new Scanner(new File(args[0]));
        } catch (FileNotFoundException e) {
            System.err.println("Error: Input file not found!");
            System.exit(1);
        }

        int n = sc.nextInt(); // Total processes

        // Every process is identified through the provided id
        // This id is not used for comparison though, it's just used for identification
        int[] ids = new int[n];
        for(int i=0; i<n; i++) {
            ids[i] = sc.nextInt();
        }

        Luby.ProcessNode[] processNodes = new Luby.ProcessNode[n];

        List<List<Integer>> processNeighbors = new ArrayList<>(); // Used for checking if MIS is indeed correct
        for(int i=0; i<n; i++) {
            List<Integer> currentProcessNeighbors = new ArrayList<>();
            for(int j=0; j<n; j++) {
                if(sc.nextInt() == 1) {
                    currentProcessNeighbors.add(ids[j]); // Add neighbors of current process
                }
            }

            // Create a new instance of a process node
            processNodes[i] = new Luby.ProcessNode(i, n, ids[i], currentProcessNeighbors);
            processNeighbors.add(currentProcessNeighbors);
        }

        Thread[] threads = new Thread[processNodes.length];

        gate = new CyclicBarrier(processNodes.length + 1); // Acts as the main thread

        // Assign a thread to each process node
        for (int i = 0; i < processNodes.length; i++) {
            threads[i] = new Thread(processNodes[i]);
            threads[i].setName(Integer.toString(ids[i])); // Process id is the name of the thread
            threads[i].start();
        }

        gate.await();

        // Main thread checks if the other threads have been terminated
        for (int i = 0; i < processNodes.length; i++) {
            while (threads[i].isAlive()) {
                System.out.println("Computing MIS...");
                threads[i].join(1000);
            }
        }
        System.out.println();

        System.out.println("Number of rounds: " + rounds);

        Set<Integer> misSet = printMIS(); // Prints processes in the MIS

        verifyMIS(n, ids, misSet, processNeighbors); // Verify correctness of the MIS
    }
}