package iqiyi;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;


/**
 * Created by gaoronglei_sx on 2017/6/30.
 */
public class TestDemo {
    public static void main(String[] args)throws IOException, InterruptedException
    {
        Resource R = new Resource();
        ThreadProducer pro = new ThreadProducer(R);
        ThreadConsumer con = new ThreadConsumer(R);
        new Thread(pro).start();
        new Thread(pro).start();
        new Thread(con).start();
        new Thread(con).start();
    }
}

class  Resource {
    public int count = 0;
  // Object lock = new Object();
    //Object conLock = new Object();

    final Lock  lock ;
    final Condition proLock;
    final Condition conLock;
    Resource()
    {
        lock = new ReentrantLock();
        proLock = lock.newCondition();
        conLock = lock.newCondition();
    }

    public void produce() {
        lock.lock();
            try {
                while (count >= 5)
                {
                    System.out.println(Thread.currentThread().getName()+".....消费线程阻塞");
                    proLock.await();
                }
                count++;
                System.out.println(Thread.currentThread().getName() + "..生产之后的数量是：。。。" + count);
                sleep(100);
                conLock.signalAll();
            } catch (InterruptedException e) {
                     e.printStackTrace();
            } finally {
                lock.unlock();
            }

//        synchronized (lock) {
//            try {
//                if(count == 5){
//                    lock.notifyAll();
//                    lock.wait();
//                }
//                count++;
//                System.out.println(Thread.currentThread().getName() + "..生产之后的数量是：。。。" + count);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } finally {
//
//            }
//        }
    }

    public void consumer() {

        lock.lock();
            try {
               if(count == 0)
               {
                   System.out.println(Thread.currentThread().getName()+".....消费线程阻塞");
                   conLock.await();
               }
                count--;
                System.out.println(Thread.currentThread().getName() + "..消费者消费之后的数量是。。。。" + count);
                sleep(100);
                proLock.signalAll();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
        }

//        synchronized (lock) {
//            try {
//                if(count == 0){
//                    lock.notifyAll();
//                    lock.wait();
//                }
//                count--;
//                System.out.println(Thread.currentThread().getName() + "..消费之后的数量是：。。。" + count);
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } finally {
//
//            }
       // }
    }
}
class ThreadProducer implements Runnable {
        private Resource r;

        ThreadProducer(Resource r) {
            this.r = r;
        }

        public void run() {
            while (true) {
                r.produce();
            }
        }
    }
class ThreadConsumer implements Runnable {
        private Resource r;

        ThreadConsumer(Resource r) {
            this.r = r;
        }

        public void run() {
            while (true) {
                r.consumer();
            }
        }

}


