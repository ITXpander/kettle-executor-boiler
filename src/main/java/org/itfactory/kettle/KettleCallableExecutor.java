package org.itfactory.kettle;

import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.LogLevel;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: puls3
 * Date: 11/27/13
 * Time: 6:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class KettleCallableExecutor implements Callable<Integer> {
    private static Logger logger = Logger.getLogger(KettleCallableExecutor.class);

    private String transPath;
    private LogLevel logLevel;

    public KettleCallableExecutor(String transPath, LogLevel logLevel) {

        this.transPath = transPath;
        this.logLevel = logLevel;
    }

    @Override
    public Integer call() throws Exception {
        return KettleExecutor.INSTANCE.executeTransformation(this.transPath, this.logLevel);
    }
}
