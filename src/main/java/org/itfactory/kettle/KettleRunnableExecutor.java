package org.itfactory.kettle;

import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.LogLevel;

/**
 * Created by puls3 on 04/08/14.
 */
public class KettleRunnableExecutor implements Runnable {
    private static Logger logger = Logger.getLogger(KettleRunnableExecutor.class);
    private static String KETTLE_JOB_SUFFIX = "kjb";
    private static String KETTLE_TRANS_SUFFIX = "ktr";

    private String path;
    private LogLevel logLevel;

    public KettleRunnableExecutor(String path, LogLevel logLevel) {
        this.path = path;
        this.logLevel = logLevel;
    }

    @Override
    public void run() {
        if(this.path.endsWith(KETTLE_TRANS_SUFFIX)) {
            KettleExecutor.INSTANCE.executeTransformation(this.path, this.logLevel);
        } else if(this.path.endsWith(KETTLE_JOB_SUFFIX)) {
            KettleExecutor.INSTANCE.executeJob(this.path, this.logLevel);
        } else {
            return;
        }
    }
}
