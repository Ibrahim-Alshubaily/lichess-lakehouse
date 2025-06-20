package com.alshubaily.chess.utils.retrieval;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class Downloader {

    public static void downloadToFile(String uri, String targetFilePath) throws IOException {
        File target = new File(targetFilePath);
        if (!tryTorrentDownload(uri, target)) {
            System.out.println("⚠️ Torrent failed. Falling back to HTTPS...");
            downloadViaHttps(uri, target);
        }
    }

    private static boolean tryTorrentDownload(String uri, File target) {
        String torrentUrl = uri + ".torrent";
        System.out.println("📡 Trying torrent: " + torrentUrl);

        try {
            boolean success = new TorrentClient()
                    .download(new URL(torrentUrl), target, 30);
            if (success && target.exists()) {
                System.out.println("✅ Torrent download complete.");
                return true;
            }
        } catch (Exception e) {
            System.out.println("❌ Torrent error: " + e.getMessage());
        }
        return false;
    }


    private static void downloadViaHttps(String uri, File target) throws IOException {
        System.out.println("⬇️ Downloading: " + uri);
        try (
                InputStream in = new URL(uri).openStream();
                FileOutputStream out = new FileOutputStream(target)
        ) {
            in.transferTo(out);
        }
        System.out.println("✅ Download complete.");
    }
}
