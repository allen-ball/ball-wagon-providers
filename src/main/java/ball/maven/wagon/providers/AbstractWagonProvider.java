/*
 * $Id$
 *
 * Copyright 2017 - 2020 Allen D. Ball.  All rights reserved.
 */
package ball.maven.wagon.providers;

import java.io.File;
import java.io.IOException;
import java.nio.file.spi.FileTypeDetector;
import java.util.Iterator;
import java.util.ServiceLoader;
import lombok.NoArgsConstructor;
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
 * @version $Revision$
 */
@NoArgsConstructor(access = PROTECTED)
public abstract class AbstractWagonProvider extends AbstractWagon {
    private String prefix = null;
    private ServiceLoader<FileTypeDetector> loader =
        ServiceLoader.load(FileTypeDetector.class,
                           getClass().getClassLoader());

    /**
     * Method to calculate bucket key prefix from
     * {@code getRepository().getBasedir()}.
     *
     * @return  The bucket key prefix.
     */
    protected String prefix() {
        if (prefix == null) {
            String basedir = strip(getRepository().getBasedir(), "/");

            prefix = isNotEmpty(basedir) ? (basedir + "/") : EMPTY;
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
