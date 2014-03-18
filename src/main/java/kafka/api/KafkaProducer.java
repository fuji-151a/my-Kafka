package kafka.api;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.joda.time.DateTime;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * KafkaProducer.
 * @author yuya
 *
 */
public class KafkaProducer {

    /** producer. */
    private Producer<String, String> producer;
    /** Topic Name. */
    private String topic;
    /** produce count. */
    private int produceCount = 0;
    /** sleep time. */
    private long sleep = 0;

    /**
     * 空のコンストラクタ.今のところ未定.
     */
    KafkaProducer() {
    }

    /**
     * コンストラクタ.Kafkaの設定ファイルを読み込む.
     * @param fileName : propertiesファイル名.
     */
    KafkaProducer(String fileName) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(fileName));
            ProducerConfig config = new ProducerConfig(prop);
            producer = new Producer<String, String>(config);
            topic = prop.getProperty("topic");
        } catch (FileNotFoundException e) {
            System.out.println("FileNotExist");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Input Error");
            e.printStackTrace();
        }
    }

    /**
     * producer.
     * @param logData : 流したいデータ
     */
    public void produce(String logData) {
        KeyedMessage<String, String> data =
                new KeyedMessage<String, String>(topic, logData);
        producer.send(data);
    }

    /**
     * propertiesFile Check.
     * @param prop : properties file
     * @return boolean
     */
    public boolean propFileCheck(Properties prop) {
        if (prop.getProperty("topic") == null
                && prop.getProperty("zk.connect") == null
                && prop.getProperty("metadata.broker.list") == null
                && prop.getProperty("serializer.class") == null) {
            System.out.println("---propertiesFile----");
            System.out.println("zk.connect=zookeeperHost:Port");
            System.out.println("topic=TopicName");
            System.out.println("metadata.broker.list=brokerHost:Port");
            System.out.println("serializer.class="
                    + "kafka.serializer.StringEncoder");
            return false;
        } else {
            return true;
        }
    }

    /**
     * set produce count.
     * @param produceCount
     */
    private void setProduceCount(int produceCount) {
        this.produceCount = produceCount;
    }

    /**
     * set sleep time.
     * @param time : millis
     */
    private void setSleepTime(long time) {
        this.sleep = time;
    }

    /**
     * 指定した件数Produceを行う.
     * @param logData
     */
    public void countProduce(String logData) {
        for (int i = 0; i < produceCount; i++) {
            produce(currentTime() + " " + i + ":" + logData);
        }
    }

    /**
     * 無限にProduceを行う.
     * @param logData
     * @throws InterruptedException
     */
    public void contProduce(String logData) throws InterruptedException {
        while (true) {
            produce(currentTime() + " " + logData);
            Thread.sleep(sleep * 1000);
        }
    }

    /**
     * 現在の時刻を[yyyy/MM/dd HH:mm:ss]形式で返す.
     * このクラスに書く必要はないw 修正予定
     * @return currentTime
     */
    private static String currentTime() {
        DateTime dt = new DateTime();
        return dt.toString("[yyyy/MM/dd HH:mm:ss]");
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Please Input:");
            System.out.println("java -cp <jarFile> kafka.api.KafkaProducer "
                    + "<propertiesFile> <mode> <produceNum>");
            System.out.println("mode:cont => 0");
            System.out.println("mode:stat => 1");
        }
        String fileName = args[0];
        int mode = Integer.parseInt(args[1]);
        String data = "Test Data";
        KafkaProducer kp = new KafkaProducer(fileName);
        switch (mode) {
            case 0:
                kp.setSleepTime(Integer.parseInt(args[2]));
                try {
                    kp.contProduce(data);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case 1:
                kp.setProduceCount(Integer.parseInt(args[2]));
                kp.countProduce(data);
                break;
            default:
                System.out.println("Please Input mode:");
                System.out.println("mode:cont => 0");
                System.out.println("mode:stat => 1");
                break;
        }

        System.out.println("System End");
    }
}
