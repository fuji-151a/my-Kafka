package kafka.api;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * KafkaProducer.
 * @author yuya
 *
 */
public class KafkaProducer {
    private String fileName;
    private Producer<String, String>producer;
    
    KafkaProducer(String fileName) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(fileName));
            ProducerConfig config = new ProducerConfig(prop);
            producer = new Producer<String, String>(config);
        } catch (FileNotFoundException e) {
            System.out.println("FileNotExist");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Input Error");
            e.printStackTrace();
        }
    }
    
    public boolean propFileCheck(Properties prop) {
        return !(prop.getProperty("topic") == null &&
        prop.getProperty("zk.connect") == null);
    }
}
