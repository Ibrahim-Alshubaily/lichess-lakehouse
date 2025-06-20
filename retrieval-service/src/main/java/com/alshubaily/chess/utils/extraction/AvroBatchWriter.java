package com.alshubaily.chess.utils.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.file.DataFileWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class AvroBatchWriter {

    private static final int BATCH_SIZE = 10_000;
    private final PGNStreamIterator source;
    private final ObjectMapper mapper = new ObjectMapper();

    public AvroBatchWriter(PGNStreamIterator source) {
        this.source = source;
    }

    public List<File> writeBatches() throws IOException {
        List<File> parts = new ArrayList<>();
        List<GenericRecord> buffer = new ArrayList<>(BATCH_SIZE);
        int part = 0;

        while (source.hasNext()) {
            buffer.add(source.next());
            if (buffer.size() >= BATCH_SIZE) {
                parts.add(writeBatchToAvro(buffer, part++));
                buffer.clear();
            }
        }
        if (!buffer.isEmpty()) {
            parts.add(writeBatchToAvro(buffer, part));
        }
        return parts;
    }

    private File writeBatchToAvro(List<GenericRecord> records, int part) throws IOException {
        if (records.isEmpty()) throw new IllegalArgumentException("Empty batch");

        GenericRecord first = records.getFirst();
        Schema schema = first.getSchema();

        String utcDate = first.get("utc_date").toString(); // "2024.05.13"
        String month = utcDate.substring(0, 7).replace('.', '-'); // "2024-05"

        Path dir = Path.of("tmp", month);
        Files.createDirectories(dir);

        String fileName = String.format("game-part-%04d.avro", part);
        File file = dir.resolve(fileName).toFile();

        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
            writer.create(schema, file);
            for (GenericRecord record : records) {
                writer.append(record);
            }
        }
        return file;
    }

}
