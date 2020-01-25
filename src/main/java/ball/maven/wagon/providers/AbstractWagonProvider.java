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

/**
 * Google Storage {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@NoArgsConstructor(access = PROTECTED)
public abstract class AbstractWagonProvider extends AbstractWagon {
    private ServiceLoader<FileTypeDetector> loader =
        ServiceLoader.load(FileTypeDetector.class,
                           getClass().getClassLoader());

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
