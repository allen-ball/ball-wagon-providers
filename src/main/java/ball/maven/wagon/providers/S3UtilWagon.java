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
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.File;
import java.io.IOException;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.component.annotations.Component;

/**
 * AWS S3 {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@Component(hint = "s3", role = Wagon.class, instantiationStrategy = "per-lookup")
@NoArgsConstructor @ToString
public class S3UtilWagon extends AbstractWagon {
    private volatile Bucket bucket = null;
    private volatile TransferManager manager = null;

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
            manager.download(bucket.getName(), source, target)
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

        postProcessListeners(resource, target, TransferEvent.REQUEST_GET);
        fireGetCompleted(resource, target);
    }

    @Override
    public boolean getIfNewer(String source, File target,
                              long timestamp) throws TransferFailedException,
                                                     ResourceDoesNotExistException,
                                                     AuthorizationException {
        fireSessionDebug("getIfNewer in "
                         + getClass().getSimpleName()
                         + " is not supported - performing an unconditional get");
        get(source, target);

        return true;
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
            manager.upload(bucket.getName(), target, source)
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
    public void putDirectory(File source,
                             String target) throws TransferFailedException,
                                                   ResourceDoesNotExistException,
                                                   AuthorizationException {
        throw new IllegalStateException();
    }

    @Override
    public boolean supportsDirectoryCopy() { return false; }
}
