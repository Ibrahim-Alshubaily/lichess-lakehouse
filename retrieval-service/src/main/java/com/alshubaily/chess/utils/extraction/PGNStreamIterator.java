package com.alshubaily.chess.utils.extraction;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PGNStreamIterator implements Iterator<GenericRecord>, Closeable {


    private static final String SCHEMA_PATH = "retrieval-service/schema/game.avsc";
    private static final Pattern MOVE_PATTERN = Pattern.compile(
            "\\b(?:O-O(?:-O)?|[KQRBN]?[a-h]?[1-8]?x?[a-h][1-8](?:=[QRNB])?)\\b"
    );
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    private static final Map<String, String> entries = new HashMap<>();
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
                    nextRecord = buildRecord();
                    return;
                }
            }

            nextRecord = null;
            close();

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

//    private boolean isEligible(Map<String, String> entries) {
//        return parseElo(entries.get("WhiteElo")) > MIN_ELO &&
//                parseElo(entries.get("BlackElo")) > MIN_ELO &&
//                entries.get("Result").matches("(1-0|0-1|1/2-1/2)");
//    }

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

    private List<String> parseMoves(String moves) {
        if (moves == null || moves.isEmpty()) return Collections.emptyList();

        List<String> parsed = new ArrayList<>();
        Matcher matcher = MOVE_PATTERN.matcher(moves);
        while (matcher.find()) {
            parsed.add(matcher.group());
        }
        return parsed;
    }

    private int parseDate(String yyyymmdd) {
        try {
            LocalDate date = LocalDate.parse(yyyymmdd, dateFormatter);
            return (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
        } catch (Exception e) {
            System.err.println("Failed to parse date: " + yyyymmdd);
            return 0;
        }
    }

    private int[] parseTimeControl(String timeControl) {
        if (timeControl == null || !timeControl.contains("+")) return new int[] {0, 0};
        try {
            String[] parts = timeControl.split("\\+");
            return new int[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
        } catch (Exception e) {
            return new int[] {0, 0};
        }
    }

    private GenericRecord buildRecord() {
        GenericRecord record = new GenericData.Record(schema);

        record.put("game_id", extractGameId(entries.get("Site")));
        record.put("utc_date", parseDate(entries.get("UTCDate")));
        record.put("white_elo", parseElo(entries.get("WhiteElo")));
        record.put("black_elo", parseElo(entries.get("BlackElo")));
        record.put("eco", entries.get("ECO"));
        record.put("opening", entries.get("Opening"));

        String timeControl = entries.get("TimeControl");
        int[] timeParts = parseTimeControl(timeControl);
        record.put("init_sec", timeParts[0]);
        record.put("inc_sec", timeParts[1]);

        record.put("termination", entries.get("Termination"));
        record.put("event", entries.get("Event"));
        record.put("outcome", parseOutcome(entries.get("Result")));
        record.put("moves", parseMoves(entries.get("moves")));
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
