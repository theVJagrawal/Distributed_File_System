package dstn;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
/**
 * Hello world!
 *
 */
public class App 
{
	
	
	
    public static void main( String[] args ) throws IOException
    {
        
     
        
      File fl=new File(System.getProperty("user.dir")+"/cifar-10/sampleSubmission.csv");
      
      FileReader fr=new FileReader(fl);
      BufferedReader bf=new BufferedReader(fr);
      String str="";
      int i=0;
     Random r=new Random();
     System.out.println(r.nextInt(1000));
        
        
       
    }
}
