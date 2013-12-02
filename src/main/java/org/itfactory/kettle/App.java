package org.itfactory.kettle;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Logger logger = Logger.getLogger(App.class);
    private static String TRANS_PATH = "/Users/puls3/Desktop/test_row_gen.ktr";
    private static int POOL_SIZE = 10;

    public static void main( String[] args )
    {
        int threadPoolSize = Integer.parseInt(args[0]);
        int threadNum = Integer.parseInt(args[1]);

        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();

        ExecutorService pool = Executors.newFixedThreadPool(threadPoolSize);
        KettleCallableExecutor kce = new KettleCallableExecutor(TRANS_PATH);

        logger.info("Starting execution...");

        for(int i =0; i< threadNum;i++) {
            pool.submit(kce);
        }

        pool.shutdown();
        logger.info("Stopping execution...");
    }
}
