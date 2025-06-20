package com.alshubaily.chess.utils.extraction;

import com.alshubaily.chess.utils.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class PGNStreamIterator implements Iterator<GenericRecord>, Closeable {

    private static final String SCHEMA_PATH = "retrieval-service/schema/game.avsc";

    private static final int MIN_ELO = 2000;
    private static final List<String> SELECTED_FIELDS = List.of(
            "WhiteElo", "BlackElo", "ECO", "Opening",
            "TimeControl", "Termination", "Event"
    );

    private final BufferedReader reader;
    private final Schema schema;
    private GenericRecord nextRecord;

    public PGNStreamIterator(InputStream in) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.schema = new Schema.Parser().parse(Files.readString(Path.of(SCHEMA_PATH)));
        advance();
    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    @Override
    public GenericRecord next() {
        GenericRecord current = nextRecord;
        advance();
        return current;
    }

    private void advance() {
        try {
            Map<String, String> entries = new HashMap<>();
            String line;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("[")) {
                    int firstQuote = line.indexOf("\"");
                    int lastQuote = line.lastIndexOf("\"");
                    if (firstQuote > 0 && lastQuote > firstQuote) {
                        String key = line.substring(1, firstQuote - 1).trim();
                        String value = line.substring(firstQuote + 1, lastQuote);
                        entries.put(key, value);
                    }
                } else if (line.startsWith("1.")) {
                    entries.put("moves", line);

                    if (!isEligible(entries)) {
                        entries.clear();
                        continue;
                    }

                    nextRecord = buildRecord(entries);
                    return;
                }
            }

            nextRecord = null;
            close();

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isEligible(Map<String, String> entries) {
        return parseElo(entries.get("WhiteElo")) > MIN_ELO &&
                parseElo(entries.get("BlackElo")) > MIN_ELO &&
                entries.get("Result").matches("(1-0|0-1|1/2-1/2)");
    }

    private int parseElo(String elo) {
        try {
            return Integer.parseInt(elo);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String parseOutcome(String result) {
        return switch (result) {
            case "1-0" -> "white";
            case "0-1" -> "black";
            case "1/2-1/2" -> "draw";
            default -> throw new IllegalArgumentException("Invalid result: " + result);
        };
    }

    private String parseMoves(String moves) {
        StringBuilder cleaned = new StringBuilder();
        for (String token : moves.split("\\s+")) {
            if (!token.matches("\\d+\\.")) {
                cleaned.append(token).append(" ");
            }
        }
        return cleaned.toString().replaceFirst("(1-0|0-1|1/2-1/2)$", "").trim();
    }

    private GenericRecord buildRecord(Map<String, String> entries) {
        GenericRecord record = new GenericData.Record(schema);

        for (String field : SELECTED_FIELDS) {
            String key = StringUtils.toSnakeCase(field);
            record.put(key, entries.get(field));
        }

        record.put("game_id", extractGameId(entries.get("Site")));
        record.put("moves", parseMoves(entries.get("moves")));
        record.put("outcome", parseOutcome(entries.get("Result")));
        record.put("white_elo", parseElo(entries.get("WhiteElo")));
        record.put("black_elo", parseElo(entries.get("BlackElo")));
        record.put("utc_date", entries.get("UTCDate"));

        return record;
    }

    private String extractGameId(String site) {
        return site.substring(site.lastIndexOf('/') + 1);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
