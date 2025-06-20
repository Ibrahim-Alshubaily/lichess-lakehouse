package com.alshubaily.chess.utils.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Set;
import java.util.stream.Collectors;

public class utils {
    public static Set<String> getBucketObjectKeys(String bucket, S3AsyncClient client) {
        try {
            return client.listObjectsV2(r -> r.bucket(bucket))
                    .thenApply(r -> r.contents().stream()
                            .map(S3Object::key)
                            .collect(Collectors.toSet()))
                    .get();
        } catch (Exception e) {
            return Set.of();
        }
    }

}
