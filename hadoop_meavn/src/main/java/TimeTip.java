public class TimeTip {
    public static void main(String[] args) throws InterruptedException {
        long timeTip;
        for (int i = 0; i < 10; i++) {
            // currentTimeMillis以每一毫秒为1
            timeTip = System.currentTimeMillis();
            System.out.println(timeTip);
            Thread.sleep(1000L);
        }

    }
}
