package cn.tanyf.testng;

import org.testng.annotations.Test;

/**
 * .
 *
 */
public class NewTest {

    /**
     * . ddd
     */
    private int i = 0;

    @Test(invocationCount = 100, threadPoolSize = 30)
    public void f() throws InterruptedException {
        System.out.println("hello testNG" + i++);
//        Thread.sleep(9999999999999L);
    }
}
