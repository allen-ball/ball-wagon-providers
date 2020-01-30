/*
 * $Id$
 *
 * Copyright 2020 Allen D. Ball.  All rights reserved.
 */
package ball.maven.wagon.providers;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.File;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.component.annotations.Component;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.strip;

/**
 * AWS S3 {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@Component(hint = "s3", role = Wagon.class, instantiationStrategy = "per-lookup")
@NoArgsConstructor @ToString
public class S3Wagon extends AbstractWagonProvider {
    private volatile Bucket bucket = null;
    private String prefix = null;
    private TransferManager manager = null;

    @Override
    protected void openConnectionInternal() throws AuthenticationException {
        try {
            if (bucket == null) {
                synchronized (this) {
                    if (bucket == null) {
                        AWSCredentialsProvider credentials =
                            new DefaultAWSCredentialsProviderChain();
                        AwsRegionProvider region =
                            new DefaultAwsRegionProviderChain();
                        AmazonS3 client =
                            AmazonS3ClientBuilder.standard()
                            .withCredentials(credentials)
                            .withRegion(region.getRegion())
                            .build();
                        String name = getRepository().getHost();

                        bucket =
                            client.listBuckets()
                            .stream()
                            .filter(t -> name.equals(t.getName()))
                            .findFirst()
                            .orElseThrow(() -> new ResourceDoesNotExistException(getRepository().toString()));

                        String basedir =
                            strip(getRepository().getBasedir(), "/");

                        prefix = isNotEmpty(basedir) ? (basedir + "/") : EMPTY;

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
            manager.download(bucket.getName(), prefix + source, target)
                .waitForCompletion();
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
            .getObjectMetadata(bucket.getName(), prefix + source);

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
            manager.upload(bucket.getName(), prefix + target, source)
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
                .doesObjectExist(bucket.getName(), prefix + name);
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
}
