package templates;

import communication.message.Message;
import communication.message.impl.event.Event;
import communication.message.impl.petrinet.PetriNet;
import communication.message.serialization.MessageSerializer;
import communication.message.serialization.deserialization.MessageFactory;
import org.springframework.core.io.ClassPathResource;
import pipeline.processingelement.Configuration;
import pipeline.processingelement.operator.MiningOperator;
import utils.Pair;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel; // ✅ missing import (caused first error)
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions; // ✅ missing import (caused second error)
import java.nio.file.attribute.FileAttribute; // ✅ required for Posix permissions on some systems
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class HeuristicsMiner extends MiningOperator<PetriNet> {

    private static final String RESOURCE_PATH = "algorithms/heuristics-miner.jar";
    /** Where we persist the miner jar (override with env HM_JAR_DIR) */
    private static final Path JAR_DIR = Paths.get(
            Objects.requireNonNullElse(System.getenv("HM_JAR_DIR"), "/opt/heuristics-miner")
    );
    private static final Path JAR_PATH = JAR_DIR.resolve("heuristics-miner.jar");

    private long eventCount = 0;
    private long lastReportTime = System.currentTimeMillis();

    private final Object processLock = new Object();
    private Process process;
    private BufferedWriter jarInput;
    private BufferedReader jarOutput;

    public HeuristicsMiner(Configuration configuration) {
        super(configuration);
        // Ensure we always clean up on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                terminate();
            } catch (Throwable ignored) {}
        }));
    }

    @Override
    protected Map<Class<? extends Message>, Integer> setConsumedInputs() {
        Map<Class<? extends Message>, Integer> map = new HashMap<>();
        map.put(Event.class, 1);
        return map;
    }

    private void ensureJarPresent() throws IOException {
        Files.createDirectories(JAR_DIR);

        // Load resource
        ClassPathResource res = new ClassPathResource(RESOURCE_PATH);
        if (!res.exists()) {
            throw new FileNotFoundException("Miner resource not found on classpath: " + RESOURCE_PATH);
        }

        // If file exists and content matches, skip write
        byte[] resSha = sha256(res.getInputStream());
        if (Files.isRegularFile(JAR_PATH)) {
            try (InputStream existing = Files.newInputStream(JAR_PATH)) {
                byte[] fileSha = sha256(existing);
                if (MessageDigest.isEqual(resSha, fileSha)) {
                    return; // already up-to-date
                }
            } catch (IOException ignored) {
                // fall through to rewrite
            }
        }

        // Atomic replace to avoid partially written binaries
        Path tmp = Files.createTempFile(JAR_DIR, "hm-", ".jar");
        try (InputStream in = res.getInputStream();
             ReadableByteChannel src = Channels.newChannel(in);
             FileChannel dst = FileChannel.open(tmp, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            dst.transferFrom(src, 0, Long.MAX_VALUE);
        }
        // make executable-ish (harmless on Windows)
        try {
            Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("r-xr-xr-x"));
        } catch (UnsupportedOperationException ignored) {}

        Files.move(tmp, JAR_PATH, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        System.out.println("[HeuristicsMiner] Jar refreshed at " + JAR_PATH);
    }

    private static byte[] sha256(InputStream in) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (DigestInputStream dis = new DigestInputStream(in, md)) {
                byte[] buf = new byte[64 * 1024];
                while (dis.read(buf) != -1) { /* consume */ }
            }
            return md.digest();
        } catch (Exception e) {
            throw new IOException("Failed to compute SHA-256", e);
        }
    }

    private void startProcess() {
        synchronized (processLock) {
            try {
                if (process != null && process.isAlive()) return;

                System.out.println("[HeuristicsMiner] Ensuring miner jar is present ...");
                ensureJarPresent();

                System.out.println("[HeuristicsMiner] Starting heuristics-miner at " + JAR_PATH + " ...");
                ProcessBuilder pb = new ProcessBuilder(
                        "java",
                        "-XX:+ExitOnOutOfMemoryError",
                        "-jar", JAR_PATH.toAbsolutePath().toString()
                );
                pb.redirectErrorStream(true);
                process = pb.start();

                jarInput  = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                jarOutput = new BufferedReader(new InputStreamReader(process.getInputStream()));

                System.out.println("[HeuristicsMiner] Process started successfully (pid=" + pidSafe(process) + ").");
            } catch (IOException e) {
                throw new RuntimeException("Failed to start heuristics-miner.jar", e);
            }
        }
    }

    private static long pidSafe(Process p) {
        try { return p.pid(); } catch (Throwable t) { return -1L; }
    }

    @Override
    protected Pair<PetriNet, Boolean> process(Message message, int portNumber) {
        synchronized (processLock) {
            try {
                startProcess();

                eventCount++;
                long now = System.currentTimeMillis();
                if (now - lastReportTime >= 1000) {
                    System.out.println("[HeuristicsMiner] Events processed in last second: " + eventCount);
                    eventCount = 0;
                    lastReportTime = now;
                }

                // Serialize event
                MessageSerializer serializer = new MessageSerializer();
                message.acceptVisitor(serializer);
                String serializedEvent = serializer.getSerialization();
                //System.out.println("[HeuristicsMiner] → miner: " + serializedEvent);

                // Send to JAR
                jarInput.write(serializedEvent);
                jarInput.newLine();
                jarInput.flush();

                // Read miner output (up to blank line) + status line
                long startTime = System.currentTimeMillis();
                StringBuilder out = new StringBuilder();
                String line;
                while ((line = jarOutput.readLine()) != null) {
                    if (line.trim().isEmpty()) break;
                    out.append(line).append(System.lineSeparator());

                    if (System.currentTimeMillis() - startTime > 10_000) {
                        System.err.println("[HeuristicsMiner] ⚠ Timeout waiting for miner output!");
                        break;
                    }
                }
                String statusLine = jarOutput.readLine();
                String content = out.toString().trim();
                boolean isSuccess = statusLine != null && Boolean.parseBoolean(statusLine);

                //System.out.println("[HeuristicsMiner] miner → content: " + content);
                //System.out.println("[HeuristicsMiner] miner → success? " + isSuccess);

                if (isSuccess && !content.isEmpty()) {
                    try {
                        PetriNet petriNet = (PetriNet) MessageFactory.deserialize(content);
                        //System.out.println("[HeuristicsMiner] ✅ Petri net deserialized successfully!");
                        return new Pair<>(petriNet, true);
                    } catch (Exception e) {
                        //System.err.println("[HeuristicsMiner] ❌ Failed to deserialize Petri net: " + e.getMessage());
                    }
                } else {
                    //System.err.println("[HeuristicsMiner] ⚠ Miner returned no valid Petri net output.");
                }
                return new Pair<>(null, false);

            } catch (Exception e) {
                System.err.println("[HeuristicsMiner] ❌ Error while processing event: " + e.getMessage());
                e.printStackTrace();

                // If the process died, clear it so we can restart on next call
                if (process != null && !process.isAlive()) {
                    safeCloseStreams();
                    process = null;
                }
                return new Pair<>(null, false);
            }
        }
    }

    @Override
    protected boolean publishCondition(Pair<PetriNet, Boolean> pair) {
        return pair.second();
    }

    @Override
    public boolean terminate() {
        super.terminate();
        synchronized (processLock) {
            try {
                safeCloseStreams();
                if (process != null) {
                    process.destroy();
                    // be nice up to 2s, then force
                    try {
                        if (!process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
                            process.destroyForcibly();
                        }
                    } catch (InterruptedException ignored) {}
                }
            } catch (Exception e) {
                System.err.println("[HeuristicsMiner] Error during terminate(): " + e.getMessage());
            } finally {
                process = null;
            }
        }
        System.out.println("[HeuristicsMiner] Process terminated.");
        return true;
    }

    private void safeCloseStreams() {
        try { if (jarInput != null) jarInput.close(); } catch (IOException ignored) {}
        try { if (jarOutput != null) jarOutput.close(); } catch (IOException ignored) {}
        jarInput = null;
        jarOutput = null;
    }
}
