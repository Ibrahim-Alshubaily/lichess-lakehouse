package com.alshubaily.chess.utils.s3;


import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.File;

public class Uploader {

    public boolean uploadFile(String bucket, File file, String prefix, S3AsyncClient baseClient) {
        createBucketIfNotExist(bucket, baseClient);
        try (S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(baseClient)
                .build()) {

            String objectKey = prefix + file.getName();
            UploadFileRequest uploadRequest = UploadFileRequest.builder()
                    .putObjectRequest(r -> r.bucket(bucket).key(objectKey))
                    .addTransferListener(LoggingTransferListener.create())
                    .source(file.toPath())
                    .build();

            FileUpload upload = transferManager.uploadFile(uploadRequest);
            upload.completionFuture().join();  // block until complete
            System.out.println("✅ Uploaded: " + bucket + "/" + objectKey);
            return true;

        } catch (Exception e) {
            System.err.println("❌ Upload failed: " + e.getMessage());
            return false;
        }
    }

    private void createBucketIfNotExist(String bucket, S3AsyncClient client) {
        try {
            client.headBucket(r -> r.bucket(bucket)).join();
        } catch (Exception e) {
            client.createBucket(r -> r.bucket(bucket)).join();
            System.out.println("🪣 Created bucket: " + bucket);
        }
    }
}
