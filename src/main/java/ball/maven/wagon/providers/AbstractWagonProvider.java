package ball.maven.wagon.providers;
/*-
 * ##########################################################################
 * Maven Wagon Providers
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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.spi.FileTypeDetector;
import java.util.Iterator;
import java.util.ServiceLoader;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authorization.AuthorizationException;

import static lombok.AccessLevel.PROTECTED;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.strip;

/**
 * Abstract {@link org.apache.maven.wagon.Wagon} which provides a
 * {@link #probeContentType(File)} method.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 */
@NoArgsConstructor(access = PROTECTED) @Slf4j
public abstract class AbstractWagonProvider extends AbstractWagon {

    /**
     * {@link #DELIMITER} = {@value #DELIMITER}
     */
    protected static final String DELIMITER = "/";

    private URI uri = null;
    private String prefix = null;
    private ServiceLoader<FileTypeDetector> loader =
        ServiceLoader.load(FileTypeDetector.class,
                           getClass().getClassLoader());

    private URI getURI() {
        if (uri == null) {
            URI ssp = URI.create(getRepository().getUrl());

            if (ssp.isOpaque()) {
                ssp = URI.create(ssp.getSchemeSpecificPart());
            }

            uri = ssp;
        }

        return uri;
    }

    /**
     * Method to get host from a possibly opaque
     * {@code getRepository().getHost()}.
     *
     * @return  The host.
     */
    protected String getHost() { return getURI().getHost(); }

    /**
     * Method to get path from a possibly opaque
     * {@code getRepository().getBasedir()}.
     *
     * @return  The path.
     */
    protected String getPath() { return getURI().getPath(); }

    /**
     * Method to calculate bucket key prefix from
     * {@code getRepository().getBasedir()}.
     *
     * @return  The bucket key prefix.
     */
    protected String prefix() {
        if (prefix == null) {
            String basedir = strip(getPath(), DELIMITER);

            prefix = isNotEmpty(basedir) ? (basedir + DELIMITER) : EMPTY;
        }

        return prefix;
    }

    /**
     * Provides functionality similar to
     * {@link java.nio.file.Files#probeContentType(Path)} but searches this
     * {@code classpath} for {@link FileTypeDetector} implementations.
     *
     * @param   file            The {@link File} to probe.
     *
     * @return  The content type of the file, or null if the content type
     *          cannot be determined.
     */
    protected String probeContentType(File file) {
        String type = null;
        Iterator<FileTypeDetector> iterator = loader.iterator();

        while (iterator.hasNext()) {
            try {
                type = iterator.next().probeContentType(file.toPath());

                if (type != null) {
                    break;
                }
            } catch (IOException exception) {
                continue;
            }
        }

        return type;
    }

    @Override
    public boolean supportsDirectoryCopy() { return false; }

    @Override
    public void putDirectory(File source,
                             String target) throws TransferFailedException,
                                                   ResourceDoesNotExistException,
                                                   AuthorizationException {
        throw new IllegalStateException();
    }
}
