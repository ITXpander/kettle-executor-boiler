package org.itfactory.kettle;

import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.LogLevel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Logger logger = Logger.getLogger(App.class);

    public static void main( String[] args )
    {
        String transPath = args[0];
        int threadPoolSize = Integer.parseInt(args[1]);
        int threadNum = Integer.parseInt(args[2]);

        ExecutorService pool = Executors.newFixedThreadPool(threadPoolSize);
        KettleCallableExecutor kce = new KettleCallableExecutor(transPath, LogLevel.BASIC);

        logger.debug("Starting execution...");

        for(int i =0; i< threadNum;i++) {
            pool.submit(kce);
            logger.debug("Started Thread: " + i);
        }

        pool.shutdown();
        logger.debug("Stopping execution...");
    }
}
