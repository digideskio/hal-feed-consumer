package com.qmetric.feed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

class ShutdownProcedure implements Runnable
{

    private static final Logger log = LoggerFactory.getLogger(ShutdownProcedure.class);

    private final ExecutorService executorService;

    private final Thread shutdownThread;

    public ShutdownProcedure(ExecutorService executorService)
    {
        this.executorService = executorService;
        shutdownThread = new Thread(this, format("shutdown-procedure:%s", executorService));
    }

    public void registerShutdownHook()
    {
        getRuntime().addShutdownHook(shutdownThread);
    }

    public void removeShutdownHook()
    {
        getRuntime().removeShutdownHook(shutdownThread);
    }

    @Override public void run()
    {
        log.info("Shutdown started");
        if (!executorService.isTerminated())
        {
            terminateExecutor();
        }
        else
        {
            log.info("executor-service is already terminated");
        }
        log.info("Shutdown completed");
    }

    public void runAndRemoveHook()
    {
        run();
        removeShutdownHook();
    }

    private void terminateExecutor()
    {
        try
        {
            stopAcceptingNewJobs();
            waitJobsToTerminate();
        }
        catch (InterruptedException e)
        {
            forceShutdown();
        }
    }

    private void stopAcceptingNewJobs()
    {
        log.info("No new jobs accepted");
        if (!executorService.isShutdown())
        {
            executorService.shutdown();
        }
        else
        {
            log.info("executor-service is already shutdown");
        }
    }

    private void waitJobsToTerminate() throws InterruptedException
    {
        log.info("Terminating all executor-service jobs");
        if (executorService.awaitTermination(30, SECONDS))
        {
            log.info("All jobs terminated normally");
        }
        else
        {
            log.warn("Running jobs did not complete within timeout. Forcing shutdown.");
            executorService.shutdownNow();
        }
    }

    private void forceShutdown()
    {
        log.warn("Shutdown thread was interrupted. Forcing executor shutdown.");
        executorService.shutdownNow();
        // Preserve interrupt status
        currentThread().interrupt();
    }
}