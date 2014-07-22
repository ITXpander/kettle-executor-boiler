package org.itfactory.kettle;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.pentaho.di.core.logging.LogLevel;
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
    private static String DEFAULT_TRANS_PATH = "/Users/puls3/Desktop/test_row_gen.ktr";
    private static int DEFAULT_POOL_SIZE = 1;
    private static int DEFAULT_THREAD_NUM = 1;

    public static void main( String[] args )
    {
        String path = DEFAULT_TRANS_PATH;
        int threadPoolSize = DEFAULT_POOL_SIZE;
        int threadNum = DEFAULT_THREAD_NUM;

        if(args.length > 0) {
            path = args[0];
            threadPoolSize = Integer.parseInt(args[1]);
            threadNum = Integer.parseInt(args[2]);
        }

        ExecutorService pool = Executors.newFixedThreadPool(threadPoolSize);
        KettleCallableExecutor kce = new KettleCallableExecutor(path, LogLevel.BASIC);

        for(int i =0; i< threadNum;i++) {
            pool.submit(kce);
        }

        pool.shutdown();
    }
}
