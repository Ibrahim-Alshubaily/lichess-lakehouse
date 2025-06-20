package com.alshubaily.chess.utils.retrieval;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

public class LichessUrlGenerator {

    private static final String BASE_URL = "https://database.lichess.org/standard/";
    public static final YearMonth FROM = YearMonth.of(2013, 1);
    public static final YearMonth TO = YearMonth.of(2025, 5);

    public static List<String> generateUrls() {
        List<String> urls = new ArrayList<>();
        YearMonth current = FROM;
        while (!current.isAfter(TO)) {
            urls.add(BASE_URL + "lichess_db_standard_rated_" + current + ".pgn.zst");
            current = current.plusMonths(1);
        }
        return urls;
    }

    public static void main(String[] args) {
        generateUrls().forEach(System.out::println);
    }
}
