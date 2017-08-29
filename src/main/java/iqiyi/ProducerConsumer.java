package iqiyi;

/**
 * Created by gaoronglei_sx on 2017/8/22.
 */

public class ProducerConsumer {

    public static void main(String[] args) {
        Stack stack = new Stack();
        Thread producer1 = new Thread(new Producer(stack), "producer1");
        Thread consumer1 = new Thread(new Consumer(stack), "consumer1");
        producer1.start();
        consumer1.start();
    }
}

class Pizza {
    public int id;

    Pizza(int id) {
        this.id = id;
    }

    public String toString() {
        return ("Pizza:" + id);
    }
}

class Stack {
    int index = 0;
    Pizza[] arrPizza = new Pizza[10];

    public synchronized void push(Pizza pizza) {
        while (index == arrPizza.length) {// 要在Stack里进行循环判断，因为线程每次是访问Stack的同一个对象
            try {
                this.wait();// 使访问这个对象的线程进入等待状态
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.notify();// 唤醒访问当前对象的其他一个线程
        arrPizza[index] = pizza;
        index++;
        System.out.println(Thread.currentThread().getName() + "生产了" + pizza);// 输出信息放在synchronized声明的方法这里，不然容易被其他线程打断，输出错误

    }

    public synchronized Pizza pull() {
        while (index == 0) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.notify();
        index--;
        Pizza pizza = arrPizza[index];
        System.out.println(Thread.currentThread().getName() + "消费了" + pizza);// 输出信息放在synchronized声明的方法这里，不然容易被其他线程打断，输出错误
        return pizza;
    }
}

class Producer implements Runnable {
    Stack stack;

    Producer(Stack stack) {
        this.stack = stack;
    }

    public void run() {
        for (int i = 0; i < stack.arrPizza.length; i++) {
            Pizza pizza = new Pizza(i);
            stack.push(pizza);
            // Thread.yield();//让出cpu给其他线程
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

class Consumer implements Runnable {
    Stack stack;

    Consumer(Stack stack) {
        this.stack = stack;
    }

    public void run() {
        for (int i = 0; i < stack.arrPizza.length; i++) {
            stack.pull();
            // Thread.yield();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}



