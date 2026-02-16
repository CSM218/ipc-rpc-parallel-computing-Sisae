package pdc;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    private static final String MAGIC = Message.CSM218_MAGIC;
    private static final int VERSION = 1;
    private static final int MAX_FRAME_BYTES = 32 * 1024 * 1024;

    private final Object writeLock = new Object();
// TODO: Implement the cluster join protocol
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private String configuredWorkerId;
    private int taskLogCount = 0;
    private static final int MAX_TASK_LOGS = 3;

    private final ExecutorService workerThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private Message readMessage(DataInputStream in) throws IOException {
        int frameLength = in.readInt();
        if (frameLength <= 0 || frameLength > MAX_FRAME_BYTES) {
            throw new IOException("Invalid frame length: " + frameLength);
        }
        byte[] frame = new byte[frameLength];
        in.readFully(frame);
        return Message.unpack(frame);
    }

    private void sendMessage(DataOutputStream out, Message message) throws IOException {
        byte[] packed = message.pack();
        out.writeInt(packed.length);
        out.write(packed);
        out.flush();
    }

    private Message createMessage(String type, String sender, byte[] payload) {
        Message message = new Message();
        message.magic = MAGIC;
        message.version = VERSION;
        message.messageType = type;
        message.studentId = sender;
        message.type = type;
        message.sender = sender;
        message.timestamp = System.currentTimeMillis();
        message.payload = payload;
        return message;
    }

    private byte[] buildRegisterPayload(String workerId, int cores, long memory) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream payloadOut = new DataOutputStream(baos);
        payloadOut.writeUTF(workerId);
        payloadOut.writeInt(cores);
        payloadOut.writeLong(memory);
        payloadOut.flush();
        return baos.toByteArray();
    }

    private byte[] buildResultPayload(int row, int col, int[][] resultBlock) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream payloadOut = new DataOutputStream(baos);
        payloadOut.writeInt(row);
        payloadOut.writeInt(col);
        payloadOut.writeInt(resultBlock.length);
        payloadOut.writeInt(resultBlock[0].length);
        for (int[] r : resultBlock) {
            for (int value : r) {
                payloadOut.writeInt(value);
            }
        }
        payloadOut.flush();
        return baos.toByteArray();
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            String WorkerId = configuredWorkerId != null ? configuredWorkerId : "WORKER-" + Thread.currentThread().getId();
            
            int cores = Runtime.getRuntime().availableProcessors();
            long memory = Runtime.getRuntime().maxMemory();

            byte[] registerPayload = buildRegisterPayload(WorkerId, cores, memory);
            sendMessage(out, createMessage("REGISTER", WorkerId, registerPayload));

            System.out.println("Registered with Master as " + WorkerId);
            
            Message response = readMessage(in);
            String responseText = new String(response.payload, StandardCharsets.UTF_8);
            
            if ("WELCOME".equals(response.type) && responseText.startsWith("Welcome ")) {
                System.out.println("Registration Successful: " + responseText);
            } else {
                System.out.println("Registration Failed: " + responseText);
                socket.close();
                return;
            }

            Message helloFromMaster = readMessage(in);
            if ("HELLO_WORKER".equals(helloFromMaster.type)) {
                sendMessage(out, createMessage("HELLO_MASTER", WorkerId, "Hello master".getBytes(StandardCharsets.UTF_8)));
            }

        } catch (IOException e) {
            System.out.println("Failed to connect with Master.");
            e.printStackTrace();}
            
    }

    private int[][] multiply(int[][] blockA, int[][] blockB) {
        int rows = blockA.length;
        int shared = blockA[0].length;
        int cols = blockB[0].length;
        int[][] result = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                for (int k = 0; k < shared; k++) {   
                result[i][j] += blockA[i][k] * blockB[k][j];
            }
        }
    }return result;

    }
    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    private void processTask(int[][] blockA, int[][] blockB, int row, int col) {
        int[][] resultBlock = multiply(blockA, blockB);
        synchronized (writeLock) {
                 try {
                        byte[] resultPayload = buildResultPayload(row, col, resultBlock);
                        sendMessage(out, createMessage("TASK_COMPLETE", "WORKER", resultPayload));

                 } catch (IOException e) {  
                     e.printStackTrace();
             }

        }
    }

    public void execute() {
         // TODO: Implement internal task scheduling
        if (in == null || out == null || socket == null || socket.isClosed()) {
            return;
        }
        try {
            while (true) {
                Message message = readMessage(in);

                if ("TASK".equals(message.type) || "RPC_REQUEST".equals(message.type)) {
                    DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(message.payload));
                    int row = payloadIn.readInt();
                    int col = payloadIn.readInt();
                    int rowsA = payloadIn.readInt();
                    int colsA = payloadIn.readInt();
                    int rowsB = payloadIn.readInt();
                    int colsB = payloadIn.readInt();
                    if (taskLogCount < MAX_TASK_LOGS) {
                        taskLogCount++;
                        System.out.println("Received matrix task from Master: row=" + row + ", col=" + col + ", blockA=" + rowsA + "x" + colsA + ", blockB=" + rowsB + "x" + colsB);
                        if (taskLogCount == MAX_TASK_LOGS) {
                            System.out.println("Further task logs suppressed.");
                        }
                    }
                    int[][] blockA = new int[rowsA][colsA];
                    for (int i = 0; i < rowsA; i++) {
                        for (int j = 0; j < colsA; j++) {
                            blockA[i][j] = payloadIn.readInt();
                        }
                    }
                    int[][] blockB = new int[rowsB][colsB];
                    for (int i = 0; i < rowsB; i++) {
                        for (int j = 0; j < colsB; j++) {
                            blockB[i][j] = payloadIn.readInt();
                        }
                    }

                    int finalRow = row;
                    int finalCol = col;
                    workerThreads.execute(() -> processTask(blockA, blockB, finalRow, finalCol));
                } else if ("PING".equals(message.type) || "HEARTBEAT".equals(message.type)) {
                    synchronized (writeLock) {
                        sendMessage(out, createMessage("PONG", "WORKER", "ok".getBytes(StandardCharsets.UTF_8)));
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Connection to Master lost");
            e.printStackTrace();
        }
    }

            private void shutdown() {
                try {
                    if (socket != null && !socket.isClosed()) {
                        socket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    public static void main(String[] args) {
        Worker worker = new Worker();
        String workerIdEnv = System.getenv("WORKER_ID");
        if (workerIdEnv != null && !workerIdEnv.isEmpty()) {
            worker.configuredWorkerId = workerIdEnv;
        }
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null || masterHost.isEmpty()) {
            masterHost = "localhost";
        }
        String masterPortEnv = System.getenv("MASTER_PORT");
        String csmBasePortEnv = System.getenv("CSM218_PORT_BASE");
        int masterPort = 5000;
        if (masterPortEnv != null && !masterPortEnv.isEmpty()) {
            masterPort = Integer.parseInt(masterPortEnv);
        } else if (csmBasePortEnv != null && !csmBasePortEnv.isEmpty()) {
            masterPort = Integer.parseInt(csmBasePortEnv);
        }
        worker.joinCluster(masterHost, masterPort);
        worker.execute();
}
}
