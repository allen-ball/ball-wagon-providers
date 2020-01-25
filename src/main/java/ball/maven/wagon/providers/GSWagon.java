/*
 * $Id$
 *
 * Copyright 2017 - 2020 Allen D. Ball.  All rights reserved.
 */
package ball.maven.wagon.providers;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.FileInputStream;
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

/**
 * Google Storage {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@Component(hint = "gs", role = Wagon.class, instantiationStrategy = "per-lookup")
@NoArgsConstructor @ToString
public class GSWagon extends AbstractWagonProvider {
    private volatile Bucket bucket = null;

    @Override
    protected void openConnectionInternal() throws AuthenticationException {
        try {
            if (bucket == null) {
                synchronized (this) {
                    if (bucket == null) {
                        bucket =
                            StorageOptions.getDefaultInstance().getService()
                            .get(getRepository().getHost());
                    }
                }
            }

            if (bucket == null) {
                throw new ResourceDoesNotExistException(getRepository().toString());
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
    public void closeConnection() { }

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
            bucket.get(source)
                .downloadTo(target.toPath());
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
        boolean newer = false;
        Blob blob = bucket.get(source);

        if (blob != null) {
            newer = blob.getUpdateTime() > timestamp;
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
            Blob blob = bucket.get(target);

            if (blob != null) {
                blob.delete();
            }

            try (FileInputStream in = new FileInputStream(source)) {
                String type = probeContentType(source);

                blob = bucket.create(target, in, type);
            }
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
        return bucket.get(name) != null;
    }
}
