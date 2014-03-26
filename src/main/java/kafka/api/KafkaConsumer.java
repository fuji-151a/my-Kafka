package kafka.api;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.google.common.collect.ImmutableMap;

/**
 * KafkaConsumer.
 * @author fuji
 *
 */
public class KafkaConsumer {

    /** consumer. */
    private ConsumerConnector consumer;
    /** topic name. */
    private String topic;

    /**
     * ConsumerConf.
     * @param fileName : propertiesFile
     */
    public KafkaConsumer(String fileName) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(fileName));
            topic = prop.getProperty("topic");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Input Error");
            e.printStackTrace();
        }
        ConsumerConfig config = new ConsumerConfig(prop);
        consumer = Consumer.createJavaConsumerConnector(config);
    }

    /**
     * Consumer.
     * @param numThreads Partition num.
     * @throws UnsupportedEncodingException : 指定された文字セットがサポートされてない場合
     */
    public void consume(int numThreads) throws UnsupportedEncodingException {
        Map<String, Integer> topicCountMap = ImmutableMap.of(topic, numThreads);
        KafkaStream<byte[], byte[]> stream
            = consumer.createMessageStreams(topicCountMap).get(topic).get(0);
        for (MessageAndMetadata<byte[], byte[]> msg : stream) {
            String data = new String(msg.message(), "UTF-8");
            System.out.println(data);
        }
    }

    /**
     * propertiesFile Check.
     * @param prop : properties file
     * @return boolean
     */
    public boolean propFileCheck(Properties prop) {
        if (prop.getProperty("group.id") == null
                && prop.getProperty("zookeeper.connect") == null
                && prop.getProperty("topic") == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * args[0]:properties　file.
     * args[1]:Thread number
     * @param args : 引数
     * @throws NumberFormatException : 文字列形式が正しくない場合
     * @throws UnsupportedEncodingException : 指定された文字セットがサポートされてない場合
     */
    public static void main(String args[]) throws NumberFormatException, UnsupportedEncodingException {
        if (args.length < 2) {
            System.out.println("Please Input:");
            System.out.println("java -cp <JarFile> kafka.api.KafkaConsumer "
                    + "<propertiesFile> <numThreads>");
        }
        String fileName = args[0];
        int threadNum = Integer.valueOf(args[1]);
        KafkaConsumer kc = new KafkaConsumer(fileName);
        kc.consume(threadNum);
    }
}
