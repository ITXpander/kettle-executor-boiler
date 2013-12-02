package org.itfactory.kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingRegistry;
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

    public int executeTransformation(String transPath) {
        int errors = 0;

        try {
            // Initialize the transformation
            TransMeta transMeta = new TransMeta(transPath, (Repository) null);

            Trans trans = new Trans(transMeta);
            trans.setLogLevel(LogLevel.NOTHING);
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
        } catch (KettleXMLException e) {
            e.printStackTrace();
        } catch (KettleException e) {
            e.printStackTrace();
        }

        return errors;
    }
}
