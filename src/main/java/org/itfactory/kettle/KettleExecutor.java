package org.itfactory.kettle;

import org.apache.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: puls3
 * Date: 11/27/13
 * Time: 6:06 PM
 * To change this template use File | Settings | File Templates.
 */
public enum KettleExecutor {
    INSTANCE;

    KettleExecutor() {
        try {
            KettleEnvironment.init();
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    public int executeTransformation(String transPath, LogLevel logLevel) {
        int errors = 0;
        Trans trans = null;

        try {
            Logger logger = Logger.getLogger(LogWriter.STRING_PENTAHO_DI_LOGGER_NAME);
            LogWriter.getInstance().removeAppender(logger.getAppender(LogWriter.STRING_PENTAHO_DI_CONSOLE_APPENDER));

            // Initialize the transformation
            TransMeta transMeta = new TransMeta(transPath, (Repository) null);

            trans = new Trans(transMeta);
            trans.setLogLevel(logLevel);
            trans.setContainerObjectId(UUID.randomUUID().toString());
            trans.prepareExecution(null);

            // start and wait until it finishes
            trans.startThreads();
            trans.waitUntilFinished();

            // Remove the logging records
            String logChannelId = trans.getLogChannelId();
            CentralLogStore.discardLines(logChannelId, false);
            CentralLogStore.discardLines(transMeta.getLogChannelId(), false);
            LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);

            errors = trans.getErrors();
        } catch (KettleException e) {
            e.printStackTrace();
        } finally {
            trans.cleanup();
        }

        return errors;
    }

    public int executeJob(String jobPath, LogLevel logLevel) {
        int errors = 0;
        Job job = null;

        try {
            // Initialize the transformation
            JobMeta jobMeta = new JobMeta(jobPath, null);

            job = new Job(null, jobMeta);
            job.setLogLevel(logLevel);
            job.setContainerObjectId(UUID.randomUUID().toString());

            // start and wait until it finishes
            job.start();
            job.waitUntilFinished();

            // Remove the logging records
            String logChannelId = job.getLogChannelId();
            CentralLogStore.discardLines(logChannelId, false);
            CentralLogStore.discardLines(jobMeta.getLogChannelId(), false);
            LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);

            errors = job.getErrors();
        } catch (KettleException e) {
            e.printStackTrace();
        }

        return errors;

    }
}
