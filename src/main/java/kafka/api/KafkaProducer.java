package kafka.api;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

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
            return false;
        } else {
            System.out.println("---propertiesFile----");
            System.out.println("zk.connect=zookeeperHost:Port");
            System.out.println("topic=TopicName");
            System.out.println("metadata.broker.list=brokerHost:Port");
            System.out.println("serializer.class="
                    + "kafka.serializer.StringEncoder");
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
     * 指定した件数Produceを行う.
     * @param logData
     */
    public void countProduce(String logData) {
        for (int i = 0; i < produceCount; i++) {
            produce(i + ":" + logData);
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Please Input:");
            System.out.println("java -cp <jarFile> kafka.api.KafkaProducer "
                    + "<propertiesFile> <produceNum>");
        }
        String fileName = args[0];
        String data = "Test Data";
        KafkaProducer kp = new KafkaProducer(fileName);
        kp.setProduceCount(Integer.parseInt(args[1]));
        kp.countProduce(data);
        System.out.println("System End");
    }
}
