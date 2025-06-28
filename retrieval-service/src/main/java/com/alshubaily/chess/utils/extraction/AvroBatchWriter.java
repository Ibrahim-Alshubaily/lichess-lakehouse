package com.alshubaily.chess.utils.extraction;


import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.file.DataFileWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class AvroBatchWriter {


    private static final int BATCH_SIZE = 500_000;
    private final PGNStreamIterator source;

    public AvroBatchWriter(PGNStreamIterator source) {
        this.source = source;
    }

    public List<File> writeBatches(Path dir) throws IOException {
        List<File> parts = new ArrayList<>();
        List<GenericRecord> buffer = new ArrayList<>();
        int part = 0;
        while (source.hasNext()) {
            buffer.add(source.next());
            if (buffer.size() >= BATCH_SIZE) {
                parts.add(writeBatchToAvro(buffer, part++, dir));
                buffer.clear();
            }
        }
        if (!buffer.isEmpty()) {
            parts.add(writeBatchToAvro(buffer, part, dir));
        }
        return parts;
    }

    private File writeBatchToAvro(List<GenericRecord> records, int part, Path dir) throws IOException {
        if (records.isEmpty()) throw new IllegalArgumentException("Empty batch");

        GenericRecord first = records.getFirst();
        Schema schema = first.getSchema();

        String fileName = String.format("game-part-%04d.avro", part);
        File file = dir.resolve(fileName).toFile();

        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
            writer.setCodec(CodecFactory.snappyCodec());
            writer.create(schema, file);
            for (GenericRecord record : records) {
                writer.append(record);
            }
        }
        return file;
    }

}
