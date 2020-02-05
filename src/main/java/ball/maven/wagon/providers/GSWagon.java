package ball.maven.wagon.providers;
/*-
 * ##########################################################################
 * Maven Wagon Providers
 * $Id$
 * $HeadURL$
 * %%
 * Copyright (C) 2017 - 2020 Allen D. Ball
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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.component.annotations.Component;

import static com.google.cloud.storage.Storage.BlobListOption;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.strip;

/**
 * Google Storage {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@Component(hint = "gs", role = Wagon.class, instantiationStrategy = "per-lookup")
@NoArgsConstructor @ToString @Slf4j
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
                            .get(getHost());
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
            if (resourceExists(source)) {
                bucket.get(prefix() + source)
                    .downloadTo(target.toPath());
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
        Blob blob = bucket.get(prefix() + source);

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
            Blob blob = bucket.get(prefix() + target);

            if (blob != null) {
                blob.delete();
            }

            try (FileInputStream in = new FileInputStream(source)) {
                String type = probeContentType(source);

                blob = bucket.create(prefix() + target, in, type);
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
        return bucket.get(prefix() + name) != null;
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

        BlobListOption[] options = new BlobListOption[] {
            BlobListOption.currentDirectory(),
            BlobListOption.prefix(prefix)
        };

        try {
            for (Blob blob : bucket.list(options).iterateAll()) {
                String key = blob.getName().substring(prefix.length());

                if (isNotEmpty(key)) {
                    String[] entries = key.split(Pattern.quote(DELIMITER), 2);

                    set.add(entries[0]
                            + (blob.isDirectory() ? DELIMITER : EMPTY));
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
}
