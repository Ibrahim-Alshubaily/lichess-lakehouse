package com.alshubaily.chess.service;

import com.alshubaily.chess.utils.kafka.Observer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;

import static com.alshubaily.chess.utils.kafka.Config.INGEST_TOPIC;

public class IngestionService {

    private static final String PREFIX = "s3a://processed-data/";
    private static final Duration BUFFER_DURATION = Duration.ofMinutes(5);

    private final Observer observer = new Observer(INGEST_TOPIC);
    private final Set<String> buffer = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public void startAsync() {
        observer.register(buffer::add);
        observer.start();

        scheduler.scheduleAtFixedRate(this::flushBuffer, 1,
                BUFFER_DURATION.toMinutes(), TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) return;

        try {
            String[] months = buffer.toArray(new String[0]);
            buffer.clear();

            String[] args = new String[5 + months.length];
            args[0] = "/usr/local/bin/docker";
            args[1] = "exec";
            args[2] = "spark";
            args[3] = "/opt/spark/bin/spark-submit";
            args[4] = "/app/scripts/ingest.py";
            for (int i = 0; i < months.length; i++) {
                args[5 + i] = PREFIX + months[i];
            }

            new ProcessBuilder(args)
                    .inheritIO()
                    .start()
                    .waitFor();

            System.out.printf("✅ Ingested batch: %s%n", String.join(", ", months));
        } catch (Exception e) {
            System.err.printf("❌ Ingestion failed: %s%n", e.getMessage());
        }
    }

    public void close() {
        observer.close();
        scheduler.shutdownNow();
    }
}
