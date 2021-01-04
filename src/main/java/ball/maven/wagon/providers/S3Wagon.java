package ball.maven.wagon.providers;
/*-
 * ##########################################################################
 * Maven Wagon Providers
 * $Id$
 * $HeadURL$
 * %%
 * Copyright (C) 2017 - 2021 Allen D. Ball
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ##########################################################################
 */
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.AwsProfileRegionProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.AwsRegionProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.File;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;
import javax.inject.Named;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.strip;

/**
 * AWS S3 {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@Named("s3")
@NoArgsConstructor @ToString @Slf4j
public class S3Wagon extends AbstractWagonProvider {
    @Getter @Setter private String profile = null;
    @Getter @Setter private String region = null;
    private volatile Bucket bucket = null;
    private TransferManager manager = null;

    @Override
    protected void openConnectionInternal() throws AuthenticationException {
        try {
            if (bucket == null) {
                synchronized (this) {
                    if (bucket == null) {
                        AmazonS3 client =
                            AmazonS3ClientBuilder.standard()
                            .withCredentials(new CredentialsProviderChain())
                            .withRegion(new RegionProviderChain().getRegion())
                            .build();
                        String name = getHost();

                        bucket =
                            client.listBuckets()
                            .stream()
                            .filter(t -> name.equals(t.getName()))
                            .findFirst()
                            .orElseThrow(() -> new ResourceDoesNotExistException(getRepository().toString()));
                        manager =
                            TransferManagerBuilder.standard()
                            .withS3Client(client)
                            .build();
                    }
                }
            }
        } catch (Exception exception) {
            if (exception instanceof AuthenticationException) {
                throw (AuthenticationException) exception;
            } else {
                throw new AuthenticationException(getRepository().toString(),
                                                  exception);
            }
        }
    }

    @Override
    public void closeConnection() {
        TransferManager manager = null;

        synchronized (this) {
            manager = this.manager;
            this.manager = null;
        }

        if (manager != null) {
            manager.shutdownNow();
        }
    }

    @Override
    public void get(String source,
                    File target) throws TransferFailedException,
                                        ResourceDoesNotExistException,
                                        AuthorizationException {
        Resource resource = new Resource(source);

        fireGetInitiated(resource, target);
        createParentDirectories(target);
        fireGetStarted(resource, target);

        try {
            if (resourceExists(source)) {
                manager.download(bucket.getName(), prefix() + source, target)
                    .waitForCompletion();
            } else {
                throw new ResourceDoesNotExistException(source);
            }
        } catch (Exception exception) {
            if (exception instanceof TransferFailedException) {
                throw (TransferFailedException) exception;
            } else if (exception instanceof ResourceDoesNotExistException) {
                throw (ResourceDoesNotExistException) exception;
            } else if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            } else {
                throw new TransferFailedException(target + " <- " + source,
                                                  exception);
            }
        }

        postProcessListeners(resource, target, TransferEvent.REQUEST_GET);
        fireGetCompleted(resource, target);
    }

    @Override
    public boolean getIfNewer(String source, File target,
                              long timestamp) throws TransferFailedException,
                                                     ResourceDoesNotExistException,
                                                     AuthorizationException {
        boolean newer = false;
        ObjectMetadata metadata =
            manager.getAmazonS3Client()
            .getObjectMetadata(bucket.getName(), prefix() + source);

        if (metadata != null) {
            newer = metadata.getLastModified().getTime() > timestamp;
        }

        if (newer) {
            get(source, target);
        }

        return newer;
    }

    @Override
    public void put(File source,
                    String target) throws TransferFailedException,
                                          ResourceDoesNotExistException,
                                          AuthorizationException {
        Resource resource = new Resource(target);

        resource.setContentLength(source.length());
        resource.setLastModified(source.lastModified());

        firePutInitiated(resource, source);
        firePutStarted(resource, source);

        try {
            manager.upload(bucket.getName(), prefix() + target, source)
                .waitForCompletion();
        } catch (Exception exception) {
            if (exception instanceof TransferFailedException) {
                throw (TransferFailedException) exception;
            } else if (exception instanceof ResourceDoesNotExistException) {
                throw (ResourceDoesNotExistException) exception;
            } else if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            } else {
                throw new TransferFailedException(source + " -> " + target,
                                                  exception);
            }
        }

        postProcessListeners(resource, source, TransferEvent.REQUEST_PUT);
        firePutCompleted(resource, source);
    }

    @Override
    public boolean resourceExists(String name) throws TransferFailedException,
                                                      AuthorizationException {
        boolean exists = false;

        try {
            exists =
                manager.getAmazonS3Client()
                .doesObjectExist(bucket.getName(), prefix() + name);
        } catch (Exception exception) {
            if (exception instanceof TransferFailedException) {
                throw (TransferFailedException) exception;
            } else if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            } else {
                throw new TransferFailedException(name, exception);
            }
        }

        return exists;
    }

    @Override
    public List<String> getFileList(String name) throws TransferFailedException,
                                                        ResourceDoesNotExistException,
                                                        AuthorizationException {
        TreeSet<String> set = new TreeSet<>();
        String prefix = prefix();

        name = strip(name, DELIMITER);

        if (isNotEmpty(name)) {
            prefix += name + DELIMITER;
        }

        try {
            ObjectListing listing =
                manager.getAmazonS3Client()
                .listObjects(bucket.getName(), defaultIfEmpty(prefix, null));

            for (;;) {
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    String key = summary.getKey().substring(prefix.length());
                    String[] entries = key.split(Pattern.quote(DELIMITER), 2);

                    set.add(entries[0]
                            + ((entries.length > 1) ? DELIMITER : EMPTY));
                }

                if (listing.isTruncated()) {
                    listing =
                        manager.getAmazonS3Client()
                        .listNextBatchOfObjects(listing);
                } else {
                    break;
                }
            }
        } catch (Exception exception) {
            if (exception instanceof TransferFailedException) {
                throw (TransferFailedException) exception;
            } else if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            } else {
                throw new TransferFailedException(name, exception);
            }
        }

        return set.stream().collect(toList());
    }

    @ToString
    private class CredentialsProviderChain extends AWSCredentialsProviderChain {
        public CredentialsProviderChain() {
            super(new ProfileCredentialsProvider(S3Wagon.this.profile),
                  DefaultAWSCredentialsProviderChain.getInstance());
        }
    }

    @ToString
    private class RegionProviderChain extends AwsRegionProviderChain {
        public RegionProviderChain() {
            super(new RegionProvider(),
                  new AwsProfileRegionProvider(S3Wagon.this.profile),
                  new DefaultAwsRegionProviderChain());
        }
    }

    @ToString
    private class RegionProvider extends AwsRegionProvider {
        public RegionProvider() { super(); }

        @Override
        public String getRegion() { return S3Wagon.this.region; }
    }
}
