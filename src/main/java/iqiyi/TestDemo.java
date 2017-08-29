package iqiyi;

import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Created by gaoronglei_sx on 2017/6/30.
 */
public class TestDemo {

    static Map<String,String> longyuanMap = new LinkedHashMap<String, String>();
    static Map<String,String> dateMap = new LinkedHashMap<String, String>();

    public static void main(String[] args)throws IOException
    {
        Resource R = new Resource();
        new Thread(new ThreadProducer(R)).start();
        new Thread(new ThreadProducer(R)).start();
        new Thread(new ThreadConsumer(R)).start();
        new Thread(new ThreadConsumer(R)).start();
    }
}

class  Resource{
    public int count = 0;
    Object obj1 = new Object();
    Object obj2 = new Object();
    public void produce()
    {
        synchronized(obj1)
        {
            while(count >= 5) {
                try {
                    obj1.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            obj1.notifyAll();
            count++;
            System.out.println(Thread.currentThread().getName()+"..生产之后的数量是：。。。"+count);

        }
    }

    public  void consumer()
    {
        synchronized(obj2)
        {
            while (count <= 0)
            {
                try {
                    obj2.wait();
                }catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            obj2.notifyAll();
            count--;
            System.out.println(Thread.currentThread().getName()+"...消费者消费之后的数量是。。。。"+count);

        }
    }
}

//class Producer {
//    private Resource r;
//
//    Producer(Resource r) {
//        this.r = r;
//    }
//
//    public void run1() {
//        try {
//            synchronized (r) {
//                while (r.count >= 5)
//                    r.wait();
//                r.notifyAll();
//                    r.count++;
//                    System.out.println(Thread.currentThread().getName() + "生产者生产商品之后，现在的库存是....." + r.count);
//
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
//
//class Consumer {
//    private Resource r;
//    Consumer(Resource r) {
//        this.r = r;
//    }
//
//    public void run1() {
//        try {
//            synchronized (r) {
//                if(r.count > 0)
//                {
//                    r.notifyAll();
//                    r.count--;
//                    System.out.println(Thread.currentThread().getName() + "消费者消费商品后，库存是...." + r.count);
//                    r.wait();
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
//
class ThreadProducer implements Runnable{
    private Resource r;

    ThreadProducer(Resource r){
        this.r = r;
    }
    public void run() {
        while (true){
            r.produce();
        }
    }
}

class ThreadConsumer implements Runnable
{
    private Resource r;

    ThreadConsumer(Resource r){
        this.r = r;
    }
    public void run() {
        while (true){
            r.consumer();
        }
    }
}


