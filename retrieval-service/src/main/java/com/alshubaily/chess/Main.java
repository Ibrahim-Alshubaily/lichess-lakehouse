package com.alshubaily.chess;

import com.alshubaily.chess.service.ExtractionService;
import com.alshubaily.chess.service.IngestionService;
import com.alshubaily.chess.service.RetrievalService;

public class Main {
    public static void main(String[] args) {
        new IngestionService().startAsync();
        new ExtractionService().startAsync();
        new RetrievalService().run();
    }
}