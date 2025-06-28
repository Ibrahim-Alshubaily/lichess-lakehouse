package com.alshubaily.chess.service;

import com.alshubaily.chess.utils.extraction.AvroBatchWriter;
import com.alshubaily.chess.utils.extraction.PGNStreamIterator;
import com.alshubaily.chess.utils.kafka.Observer;
import com.alshubaily.chess.utils.kafka.Producer;
import com.alshubaily.chess.utils.s3.ClientProvider;
import com.alshubaily.chess.utils.s3.Uploader;
import com.alshubaily.chess.utils.s3.utils;
import com.github.luben.zstd.ZstdInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.alshubaily.chess.service.RetrievalService.DATA_DIRECTORY;
import static com.alshubaily.chess.utils.kafka.Config.INGEST_TOPIC;
import static com.alshubaily.chess.utils.kafka.Config.SOURCE_TOPIC;

public class ExtractionService {

    private static final String INPUT_BUCKET = "source-data";
    private static final String OUTPUT_BUCKET = "processed-data";

    private final Uploader uploader = new Uploader();
    private final Observer consumer = new Observer(SOURCE_TOPIC);
    private final Producer producer = new Producer(INGEST_TOPIC);

    public void startAsync() {
        consumer.register(this::process);
        consumer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private void process(String key) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(INPUT_BUCKET)
                .key(key)
                .build();

        var client = ClientProvider.INSTANCE;
        try (
                InputStream stream = client.getObject(req, AsyncResponseTransformer.toBlockingInputStream()).get();
                ZstdInputStream decompressed = new ZstdInputStream(stream);
                PGNStreamIterator iterator = new PGNStreamIterator(decompressed)
        ) {

            String month = extractMonthFromKey(key);
            Path dir = Path.of(DATA_DIRECTORY, month);
            Files.createDirectories(dir);

            AvroBatchWriter writer = new AvroBatchWriter(iterator);
            List<File> parts = writer.writeBatches(dir);
            for (File part : parts) {
                uploader.uploadFile(OUTPUT_BUCKET, part, month + "/", client);
                part.delete();
            }
            producer.send(month);
            System.out.println("✅ Extracted and uploaded: " + key);
            Files.deleteIfExists(dir);
        } catch (Exception e) {
            System.err.println("❌ Extraction failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void drive() {
        try {
            var client = ClientProvider.INSTANCE;

            List<String> keys = client.listObjectsV2(r -> r.bucket(INPUT_BUCKET))
                    .thenApply(r -> r.contents().stream()
                            .map(S3Object::key)
                            .filter(key -> key.endsWith(".pgn.zst"))
                            .toList())
                    .get();

            Set<String> processedMonths = getProcessedMonths();

            for (String key : keys) {
                String month = extractMonthFromKey(key);
                if (processedMonths.contains(month)) {
                    System.out.println("⏭️ Skipping already processed month: " + month);
                    continue;
                }

                System.out.println("Processing: " + key);
                process(key);
            }
            System.out.println("🏁 Extraction complete.");
        } catch (Exception e) {
            System.err.println("❌ Extraction failed: " + e.getMessage());
        }
    }

    private static String extractMonthFromKey(String key) {
        int start = key.lastIndexOf('_') + 1;
        int end = key.indexOf(".pgn");
        return key.substring(start, end); // e.g. "2013-09"
    }

    private Set<String> getProcessedMonths() {
        List<String> months = utils.getBucketObjectKeys(OUTPUT_BUCKET, ClientProvider.INSTANCE).stream()
                .filter(key -> key.contains("/"))
                .map(key -> key.substring(0, key.indexOf('/')))
                .distinct()
                .sorted()
                .toList();

        if (months.isEmpty()) return Set.of();
        // TODO: make month extraction atomic.
        return Set.copyOf(months.subList(0, months.size() - 1)); // drop last (might be partially completed)
    }

    private void close() {
        producer.close();
    }

}
