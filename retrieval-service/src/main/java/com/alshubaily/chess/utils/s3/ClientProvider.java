package com.alshubaily.chess.utils.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;

import static com.alshubaily.chess.utils.s3.Const.*;

public class ClientProvider {
    public static final S3AsyncClient INSTANCE = S3AsyncClient.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
            ))
            .forcePathStyle(true) // Required for MinIO
            .build();
}
