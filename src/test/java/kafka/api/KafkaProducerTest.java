package kafka.api;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class KafkaProducerTest {

    @Test
    public void testPropFileCheck() throws FileNotFoundException, IOException {
        String fileName = "test.properties";
        String file = KafkaProducerTest.class
                .getClassLoader().getResource(fileName).getPath();
        KafkaProducer kp = new KafkaProducer();
        Properties prop = new Properties();
        prop.load(new FileInputStream(file));
        assertThat(kp.propFileCheck(prop), is(true));
    }

}
