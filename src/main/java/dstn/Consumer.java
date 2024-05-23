package dstn;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
public class Consumer {

    	public static int add=0;
    	public static  volatile Scanner scr;
    	static String recieveFolder="serverrec";
    	static String send="";
    	static String bootstrap_server="10.60.4.137:9092";
        static String topic ="node1";
        static String group_id="1";
        static int i=0;
        
        static int startreq1=-1;
        static int startreq2=-1;
        
    	
        public static void main( String[] args ) throws IOException
        {
            
         
            
          
            
            
     Thread reciever=new Thread(new Runnable() {
            	
            	public void run() {
            		 Properties properties=new Properties();
         	        
         	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
         	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
         	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
         	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
         	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
         	        
         		   KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String,byte[]>(properties);
                   TopicPartition topicPartition = new TopicPartition(topic, 1);
                   consumer.assign(Collections.singletonList(topicPartition));
                 
                 
                  while(true) {
                 	
                 	ConsumerRecords<String, byte[]> records=consumer.poll(Duration.ofMillis(100));
                 	
                 	
                 	for(ConsumerRecord<String,byte[]> record:records) {
                 		
                 		byte[] file_data=record.value();
                 		try (FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir")+"/cifar-10/serverrec/"+record.key())) {
                 		    fos.write(file_data); // Write the byte array to the file
                 		    System.out.println("File successfully created");
                 		    i++;
                 		} catch (IOException e) {
                 		    e.printStackTrace();
                 		    System.out.println(record.key()+"    "+Arrays.toString(file_data));
                 		}
                 		
                 	}
                 	
                 }
                    
            	}
            });
            
            
            final Thread sender=new Thread(new Runnable() {
            	
            	public void run() {
            		
            		Properties properties=new Properties();
        	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        	        File filee=new File(System.getProperty("user.dir")+"/cifar-10/train");
        	        
        	        File[] images=filee.listFiles();
        	        
        	        KafkaProducer<String,byte[]> producer=new KafkaProducer<String, byte[]>(properties);
        	        
        	        
        	        int i=0;
    	        	for(File file:images) {
            		        byte[] filebyte;
            		        int filenint=Integer.parseInt(file.getName());
            		        if(filenint<startreq1 || filenint>startreq2)continue;
    						try {
    							
    							filebyte = Files.readAllBytes(file.toPath());
    							ProducerRecord<String,byte[]> record=new ProducerRecord<String,byte[]>(topic,1,"key",filebyte);
    	        		        
    	        		        producer.send(record,new Callback() {
    	        		
    	    						public void onCompletion(RecordMetadata metadata, Exception exception) {
    	    							// TODO Auto-generated method stub
    	    							if(exception==null) {
    	    								System.out.println("message sent successfully");
    	    							}else {
    	    								System.out.println("error in sending message");
    	    							}
    	    							
    	    						}
    	    			        	
    	    			        });
    	        		        i++;
    						} catch (IOException e) {
    							e.printStackTrace();
    						}   
            		}
    	        	
    	        	startreq1=-1;
    	        	startreq2=-1;
            	}
            });
            
            
            
           //sending commands
            
            Thread sendcommands=new Thread(new Runnable() {
    			public void run() {
    			        		
    			        		Properties properties=new Properties();
    			    	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
    			    	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
    			    	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
    			    	       
    			    	        
    			    	        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
    			    	        Scanner sc=new Scanner(System.in);
    			    	        
    			    	        while(true) {
    			    	        	System.out.println("Enter command for action :");
    				    	        String command=sc.nextLine();
    				    	        ProducerRecord<String,String> record=new ProducerRecord<String,String>(topic,0,"key",command);
    				        		producer.send(record,new Callback() {
    				        		        public void onCompletion(RecordMetadata metadata, Exception exception) {
    				    							if(exception==null) {
    				    								System.out.println("message for sending sent successfully");
    				    							}else {
    				    								System.out.println("message for sending error in sending message");
    				    							}
    				    						}
    				    			        	
    				    			 });
    			    	        }
    			    	      
    			 }
            });
            
            
            //recieving commands
    	      Thread reccommands=new Thread(new Runnable() {
    	        	
    	        	public void run() {
    	        		  Properties properties=new Properties();
    	      	        
    	      	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
    	      	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
    	      	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	      	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    	      	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    	      	        
    		        	 	KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);
    		                TopicPartition topicPartition = new TopicPartition(topic, 0);
    		                consumer.assign(Collections.singletonList(topicPartition));
    		                
    		                
    	        		while(true) {
    		                	
    		                	ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
    		                	for(ConsumerRecord<String,String> record:records) {
    		                		String rcvdcommand=record.value();
    		                		String[] str=rcvdcommand.split(" ");
    		                		String code=str[0];
    		                		startreq1=Integer.parseInt(str[1]);
    		                		startreq2=Integer.parseInt(str[2]);
    		                		sender.start();
    		                		
    		                		System.out.println(record.value());
    		                	}
    	        		}
    	        		
    	        		
    	        	}
    	        });
    	        
    	        sendcommands.start();
    	        
    	        reciever.start();
    	        
            
            
            
           
        }
    }
