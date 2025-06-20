package com.alshubaily.chess.service;

import com.alshubaily.chess.utils.Downloader;
import com.alshubaily.chess.utils.LichessUrlGenerator;
import com.alshubaily.chess.utils.s3.ClientProvider;
import com.alshubaily.chess.utils.s3.Uploader;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alshubaily.chess.utils.s3.utils.getBucketObjectKeys;

public class RetrievalService {

    private static final String DOWNLOAD_DIRECTORY = "retrieval-service/data/";
    private static final String BUCKET = "source-data";

    public void run() {
        Uploader uploader = new Uploader();

        try {
            Files.createDirectories(Path.of(DOWNLOAD_DIRECTORY));

            List<String> urls = LichessUrlGenerator.generateUrls();
            Set<String> existingFiles = getBucketObjectKeys(BUCKET, ClientProvider.INSTANCE);
            urls.removeIf(url -> {
                String fileName = url.substring(url.lastIndexOf('/') + 1);
                return existingFiles.contains(fileName);
            });

            for (String url : urls) {
                String fileName = url.substring(url.lastIndexOf('/') + 1);
                File localFile = new File(DOWNLOAD_DIRECTORY, fileName);

                try {
                    Downloader.downloadToFile(url, localFile.getAbsolutePath());
                    boolean uploaded = uploader.uploadFile(BUCKET, localFile, ClientProvider.INSTANCE);

                    if (!uploaded) throw new IOException("Upload failed");

                    if (localFile.delete()) {
                        System.out.println("✅ Uploaded and deleted: " + fileName);
                    } else {
                        System.err.println("⚠️ Uploaded but failed to delete: " + fileName);
                    }
                } catch (Exception e) {
                    System.err.println("❌ Failed on " + fileName + ": " + e.getMessage());
                    break;
                }
            }

            System.out.println("🏁 Retrieval complete.");

        } catch (Exception e) {
            System.err.println("❌ Retrieval setup failed: " + e.getMessage());
        }
    }
}
