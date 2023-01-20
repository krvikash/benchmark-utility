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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.krvikash.iceberg.benchmark.statistics.NDV;

import java.util.List;

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
        long totalSize = 0;
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(path);
        ListObjectsV2Result listObjects = client.listObjectsV2(request);
        List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            totalSize += objectSummary.getSize();
        }
        return new NDV(getDataPath(bucket, path), objectSummaries.size(), totalSize);
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