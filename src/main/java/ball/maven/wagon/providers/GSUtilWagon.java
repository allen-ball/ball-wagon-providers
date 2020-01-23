/*
 * $Id$
 *
 * Copyright 2017 - 2020 Allen D. Ball.  All rights reserved.
 */
package ball.maven.wagon.providers;

import java.io.File;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.CommandExecutionException;
import org.apache.maven.wagon.CommandExecutor;
import org.apache.maven.wagon.PathUtils;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.Streams;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.component.annotations.Component;
import org.codehaus.plexus.util.cli.CommandLineException;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.codehaus.plexus.util.cli.Commandline;

/**
 * {@link.uri https://cloud.google.com/storage/docs/gsutil/ gsutil}
 * {@link Wagon} implementation.
 *
 * @author {@link.uri mailto:ball@hcf.dev Allen D. Ball}
 * @version $Revision$
 */
@Component(hint = "gs", role = Wagon.class, instantiationStrategy = "per-lookup")
@NoArgsConstructor @ToString
public class GSUtilWagon extends AbstractWagon implements CommandExecutor {
    @Override
    protected void openConnectionInternal() throws AuthenticationException { }

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
            cp(getRepository().getUrl() + source, target);
        } catch (CommandExecutionException exception) {
            fireTransferError(resource, exception, TransferEvent.REQUEST_GET);
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

        if (! source.exists()) {
            throw new ResourceDoesNotExistException("Specified source file does not exist: "
                                                    + source);
        }

        firePutStarted(resource, source);

        try {
            cp(source, getRepository().getUrl() + target);
        } catch (CommandExecutionException exception) {
            fireTransferError(resource, exception, TransferEvent.REQUEST_PUT);
        }

        postProcessListeners(resource, source, TransferEvent.REQUEST_PUT);
        firePutCompleted(resource, source);
    }

    @Override
    public void putDirectory(File source,
                             String target) throws TransferFailedException,
                                                   ResourceDoesNotExistException,
                                                   AuthorizationException {
        try {
            cp(source,
               getRepository().getUrl() + target);
        } catch (CommandExecutionException exception) {
            throw new TransferFailedException(exception.getMessage(),
                                              exception);
        }
    }

    @Override
    public boolean supportsDirectoryCopy() { return true; }

    private void cp(File source,
                    String target) throws CommandExecutionException {
        cp(source.getAbsolutePath(), target);
    }

    private void cp(String source,
                    File target) throws CommandExecutionException,
                                        TransferFailedException {
        createParentDirectories(target);
        cp(source, target.getAbsolutePath());
    }

    private void cp(String source,
                    String target) throws CommandExecutionException {
        Commandline cl = new Commandline();

        cl.setExecutable("gsutil");
        cl.addArguments(new String[] {
                            "-m", "cp", "-n", "-r", source, target
                        });

        executeCommand(CommandLineUtils.toString(cl.getCommandline()));
    }

    @Override
    public void executeCommand(String command) throws CommandExecutionException {
        fireTransferDebug("Executing command: " + command);
        executeCommand(command, false);
    }

    @Override
    public Streams executeCommand(String command,
                                  boolean ignoreFailures) throws CommandExecutionException {
        Streams streams = null;
        Commandline cl = new Commandline(command);

        fireSessionDebug("Executing command: " + cl.toString());

        try {
            CommandLineUtils.StringStreamConsumer out =
                new CommandLineUtils.StringStreamConsumer();
            CommandLineUtils.StringStreamConsumer err =
                new CommandLineUtils.StringStreamConsumer();
            int exitCode = CommandLineUtils.executeCommandLine(cl, out, err);

            streams = new Streams();
            streams.setOut(out.getOutput());
            streams.setErr(err.getOutput());

            fireSessionDebug(streams.getOut());
            fireSessionDebug(streams.getErr());

            if (exitCode != 0) {
                if (! ignoreFailures || exitCode != 0) {
                    throw new CommandExecutionException("Exit code "
                                                        + exitCode + " - "
                                                        + err.getOutput());
                }
            }
        } catch (CommandLineException exception) {
            throw new CommandExecutionException("Error executing command line: "
                                                + command,
                                                exception);
        }

        return streams;
    }
}
