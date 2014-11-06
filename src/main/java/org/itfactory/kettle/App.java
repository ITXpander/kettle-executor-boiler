package org.itfactory.kettle;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.itfactory.kettle.concurrent.PrioritizedFutureTask;
import org.itfactory.kettle.concurrent.PrioritizedThreadPoolExecutor;
import org.pentaho.di.core.logging.LogLevel;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class App {
    private static Logger logger = Logger.getLogger(App.class);
    private static String DEFAULT_TRANS_PATH = "/Users/puls3/Desktop/test_row_gen.ktr";
    private static String DEFAULT_LOG_LEVEL = "Error";
    private static int DEFAULT_POOL_SIZE = 1;
    private static int DEFAULT_THREAD_NUM = 1;

    public static void main(String[] args) {
        String path = DEFAULT_TRANS_PATH;
        String logLevel = DEFAULT_LOG_LEVEL;
        int threadPoolSize = DEFAULT_POOL_SIZE;
        int threadNum = DEFAULT_THREAD_NUM;

        //BasicConfigurator.configure();

        if (args.length > 0) {
            path = args[0];
            threadPoolSize = Integer.parseInt(args[1]);
            threadNum = Integer.parseInt(args[2]);
            logLevel = args[3];
        }

        PriorityBlockingQueue<Runnable> pbq = new PriorityBlockingQueue<Runnable>(threadPoolSize);
        LinkedList<Future> futureList = new LinkedList<Future>();
        PrioritizedThreadPoolExecutor tpe = new PrioritizedThreadPoolExecutor(threadPoolSize, threadPoolSize + 50, 30 * 1000, TimeUnit.MILLISECONDS, pbq);

        Random r = new Random();
        logger.debug("warming up thread pool...");
        for (int i = 0; i < threadNum; i++) {
            KettleCallableExecutor kce = new KettleCallableExecutor(path, LogLevel.getLogLevelForCode(logLevel));
            futureList.add(tpe.submit(kce, r.nextInt(100)));
        }

        tpe.shutdown();

        for (Future future : futureList) {
            try {
                Integer result = (Integer) future.get();
                logger.info("result: " + result + " - priority: " + ((PrioritizedFutureTask) future).getPriority() + " completion time: " + ((PrioritizedFutureTask) future).getCompletionTime());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }


    }
}
