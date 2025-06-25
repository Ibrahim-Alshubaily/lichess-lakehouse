package com.alshubaily.chess.utils.s3;

import io.github.cdimascio.dotenv.Dotenv;

public class Const {
    private static final Dotenv dotenv = Dotenv.load();

    public static final String ENDPOINT = "http://localhost:9000";
    public static final String ACCESS_KEY = dotenv.get("MINIO_USER");
    public static final String SECRET_KEY = dotenv.get("MINIO_PASSWORD");
}
