/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.krvikash.iceberg.benchmark;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import io.krvikash.iceberg.benchmark.statistics.NDV;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.krvikash.iceberg.benchmark.BenchmarkUtils.getDataPath;

public class S3Client
{
    private final String bucket;
    private final String profile;
    private final String accessKey;
    private final String secretKey;
    private final String region;
    private AmazonS3 client;

    private S3Client(String bucket, String profile, String accessKey, String secretKey, String region)
    {
        this.bucket = bucket;
        this.profile = profile;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
    }

    public String getBucket()
    {
        return bucket;
    }

    public S3Client createS3Client()
    {
        if (profile != null) {
            AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(profile);
            this.client = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).build();
        }
        else {
            throw new IllegalArgumentException("Only profile credential provider is implemented");
        }
        return this;
    }

    public NDV getNDV(String path)
    {
        long totalFileCount = 0;
        AtomicLong totalSize = new AtomicLong();
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(path);
        ListObjectsV2Result listObjects;
        do {
            listObjects = client.listObjectsV2(request);

            listObjects.getObjectSummaries().forEach(objectSummary -> totalSize.addAndGet(objectSummary.getSize()));
            totalFileCount += listObjects.getObjectSummaries().size();

            request.setContinuationToken(listObjects.getNextContinuationToken());
        }
        while (listObjects.isTruncated());

        return new NDV(getDataPath(bucket, path), totalFileCount, totalSize.get());
    }

    public void download(String source, String target)
    {
        List<String> objectKeys = client.listObjects(bucket, source.replace("s3://" + bucket + "/", ""))
                .getObjectSummaries()
                .stream()
                .map(s3ObjectSummary -> s3ObjectSummary.getKey())
                .collect(Collectors.toList());
        try {
            for (String key : objectKeys) {
                Path path = Paths.get(target);
                Path fileName = Paths.get(key).getFileName();

                if(!Files.exists(path)) {
                    Files.createDirectories(path);
                }
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);
                S3Object s3Object = client.getObject(getObjectRequest);
                Files.copy(s3Object.getObjectContent(), Path.of(path.toString(), fileName.toString()));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder
    {
        private String bucket;
        private String profile = "default";
        private String accessKey;
        private String secretKey;
        private String region;

        public Builder withBucket(String bucket)
        {
            this.bucket = bucket;
            return this;
        }

        public Builder withProfile(String profile)
        {
            this.profile = profile;
            return this;
        }

        public Builder withAccessKey(String accessKey)
        {
            this.accessKey = accessKey;
            return this;
        }

        public Builder withSecretKey(String secretKey)
        {
            this.secretKey = secretKey;
            return this;
        }

        public Builder setRegion(String region)
        {
            this.region = region;
            return this;
        }

        public S3Client build()
        {
            return new S3Client(bucket, profile, accessKey, secretKey, region);
        }
    }
}
