package dstn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class DLServer implements Datapre{
	public static int add=0;
	public static  volatile Scanner scr;
	static String recieveFolder="mldata_prime";
	static String send="";
//	static String bootstrap_server="10.60.4.137:9092";   //change the kafka server for additional security
    static String topic ="mldata";
    static String group_id="1";
    static int i=0;
    static int[] alive=new int[4];
   
    static HashMap<String,String> metadata=new HashMap<String,String>();
    static HashMap<String,String> type=new HashMap<String,String>();
    static ArrayList<String> nodesdata=new ArrayList<>(Arrays.asList(new String[] {"node1","node2","node3"}));
    
	
    public static void main( String[] args ) throws IOException
    {
            System.out.println("setting the csv......");
    	    setthecsv();
    	    System.out.println("done with the csv......");
     
//            File folder=new File(System.getProperty("user.dir")+"/cifar-10/"+recieveFolder);
//            folder.createNewFile();
//            
      
        	Thread rancheck=new Thread(new Runnable() {
  	    	  public void run() {
  	    		  while(true) {
  	    			  System.out.println(Arrays.toString(alive));
  	    			  try {
  						Thread.sleep(10000);
  					} catch (InterruptedException e) {
  						// TODO Auto-generated catch block
  						e.printStackTrace();
  					}
  	    		  }
  	    	  }
  	      });
      	rancheck.start();
      	
      	
      	 Thread check1=new Thread(new Runnable() {
	    		
	    		public void run() {
	    			while(true) {
	    				
	    				
	    				Properties properties=new Properties();
	 	     	        
	 	     	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
	 	     	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
	 	     	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	 	     	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
	 	     	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
	 	     	        
	 	     		    KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String,byte[]>(properties);
	 	                TopicPartition topicPartition = new TopicPartition(topic, 4);
	 	                consumer.assign(Collections.singletonList(topicPartition));
	 	               int count=0;
	 	               while(true) {
	 	            	   
	 		             	ConsumerRecords<String, byte[]> records=consumer.poll(Duration.ofMillis(100));
	 		             	for(ConsumerRecord<String,byte[]> record:records) {
	 		             		alive[1]=1;
	 		             		count=0;
	 		             	}
	 		             	count++;
	 		                try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
	 		                if(count>2)alive[1]=0;
	 		         
	 		             }
	 		                
	 		        	}
	 	  	
	 	  	
	    			
	    		}
	    	});
Thread check2=new Thread(new Runnable() {
    		
    		public void run() {
    			while(true) {
    				
    				
    				Properties properties=new Properties();
 	     	        
 	     	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
 	     	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
 	     	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
 	     	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
 	     	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
 	     	        
 	     		    KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String,byte[]>(properties);
 	                TopicPartition topicPartition = new TopicPartition(topic, 5);
 	                consumer.assign(Collections.singletonList(topicPartition));
 	               int count=0;
 	               while(true) {
 	            	   
 		             	ConsumerRecords<String, byte[]> records=consumer.poll(Duration.ofMillis(100));
 		             	for(ConsumerRecord<String,byte[]> record:records) {
 		             		alive[2]=1;
 		             		count=0;
 		             	}
 		             	count++;
 		                try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
 		                if(count>2)alive[2]=0;
 		         
 		             }
 		                
 		        	}
 	  	
    			
    		}
    	});
		Thread check3=new Thread(new Runnable() {
		    		
		    		public void run() {
		    			while(true) {
		    				
		    				
		    				Properties properties=new Properties();
		 	     	        
		 	     	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
		 	     	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
		 	     	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		 	     	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
		 	     	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		 	     	        
		 	     		    KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String,byte[]>(properties);
		 	                TopicPartition topicPartition = new TopicPartition(topic, 6);
		 	                consumer.assign(Collections.singletonList(topicPartition));
		 	               int count=0;
		 	               while(true) {
		 	            	   
		 		             	ConsumerRecords<String, byte[]> records=consumer.poll(Duration.ofMillis(100));
		 		             	for(ConsumerRecord<String,byte[]> record:records) {
		 		             		alive[3]=1;
		 		             		count=0;
		 		             	}
		 		             	count++;
		 		                try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
		 		                if(count>2)alive[3]=0;
		 		         
		 		             }
		 		                
		 		        	}
		 	  	
		    			
		    		}
		    	});
		check1.start();
		check2.start();
		check3.start();
        
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
			             	
			                int createdfiles=0;
			             	for(ConsumerRecord<String,byte[]> record:records) {
			             		
			             		byte[] file_data=record.value();
			             		try (FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir")+"/cifar-10/mldata/"+recieveFolder+"/"+record.key())) {
			             		    fos.write(file_data); // Write the byte array to the file
			             		    createdfiles++;
			             		    metadata.put(record.key().substring(0,record.key().length()-4), System.getProperty("user.dir")+"/cifar-10/mldata/"+recieveFolder+"/"+record.key());
			             		} catch (IOException e) {
			             		    e.printStackTrace();
			             		    System.out.println(record.key()+"    "+Arrays.toString(file_data));
			             		}
			             		
			             	}
			             	if(createdfiles!=0)
			             	System.out.println("Total of "+createdfiles+" created");
			             	
			             }
			                
			        	}
			        });
			        
			        
		
	       
//	        reccommands.start();
	        reciever.start();
//	        sender(0,1000,"node3");
//     		try {
//				Thread.sleep(3000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
	        
	        
	        Thread askcommand=new Thread(new Runnable() {
	        	public void run() {
	        		while(true) {
		        		System.out.println("Enter command for action ");
		        		Scanner sc=new Scanner(System.in);
		     	        String command=sc.nextLine();
		     	        System.out.println("Entered");
		     	        for(int i=1;i<4;i++) {
		     	        	if(alive[i]==1) {
		     	        		
		     	        		sendcommand(command,nodesdata.get(i-1));
		     	        		break;
		     	        	}
		     	        }
		     	        
	        		}
	        	}
	        });
	        
	        askcommand.start();
	       
           

	        
	     
	        
    
       
    }
    
    
    
    
    
    
    
    private static void setthecsv() throws IOException{
    	 File fl=new File(System.getProperty("user.dir")+"/cifar-10/sampleSubmission.csv");
         
         FileReader fr;
		try {
			fr = new FileReader(fl);
			BufferedReader bf=new BufferedReader(fr);
		    String str="";
		    
		    while((str=bf.readLine())!=null) {
		       	 String[] str1=str.split(",");
		       	 type.put(str1[0],str1[1]);
		       	 
		    }
		    
		   
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
		
	}







	//For sending the commands
    
    public static void sendcommand(String command,String node) {
		
		        		Properties properties=new Properties();
		    	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
		    	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		    	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		    	       
		    	        
		    	        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
		    	        final String n=node;
		    	       
		    	        ProducerRecord<String,String> record=new ProducerRecord<String,String>(node,0,"key",command);
		        		producer.send(record,new Callback() {
		        		        public void onCompletion(RecordMetadata metadata, Exception exception) {
		    							if(exception==null) {
		    								System.out.println("Command sent successfully to "+n);
		    							}else {
		    								System.out.println("message for sending error in sending message "+exception.getMessage());
		    							}
		    						}
		    			        	
		    		    });
	}
		    	      
		 
    
    
    
    
    
    
  //For Sending the message
    
   public static void sender(int startreq1,int startreq2,String topicc) {
    		
    		Properties properties=new Properties();
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
	        File filee=new File(System.getProperty("user.dir")+"/dataset/Train");
	        
	        File[] folder=filee.listFiles();
	        
	        KafkaProducer<String,byte[]> producer=new KafkaProducer<String, byte[]>(properties);
	        
	        
	        int sentfile=0;
	        int count=0;
	        System.out.println("sending in  process.................");
	        for(File fl:folder) {
	        	if(fl.getName().length()>1)continue;
	        	String fname=fl.getName();
	        	System.out.println(System.getProperty("user.dir")+"/dataset/Train/"+fname);
	        	File[] images=new File(System.getProperty("user.dir")+"/dataset/Train/"+fname).listFiles();
	           
        	for(File img:images) {
        		 
        		  
    		        byte[] filebyte;
    		       
    		        final int[] a=new int[1];
					try {
						
						filebyte = Files.readAllBytes(img.toPath());
						String name=img.getName().substring(0,img.getName().length()-4);
						ProducerRecord<String,byte[]> record=new ProducerRecord<String,byte[]>(topicc,2,fname+"_"+img.getName(),filebyte);
        		        record.headers().add("name","value".getBytes());
        		        count++;
        		        if(count==1000) {
        		        	count=0;
        		        	try {
								Thread.sleep(3000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
        		        }
        		        producer.send(record,new Callback() {
        		
    						public void onCompletion(RecordMetadata metadata, Exception exception) {
    							// TODO Auto-generated method stub
    							if(exception==null) {
    								a[0]=1;
    								System.out.println("done in sending message");
    							}else {
    								
    								System.out.println("error in sending message");
    							}
    							
    						}
    			        	
    			        });
        		        
        		       
					} catch (IOException e) {
						e.printStackTrace();
					} 
					sentfile+=a[0];
    		}
	        }
        	System.out.println("Total of "+sentfile +" files are sent to "+topicc);
        	
        	
        	
        	
    }
   public static void senderd(int startreq1,int startreq2,String topicc) {
		
	   Properties properties=new Properties();
       properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
       properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
       File filee=new File(System.getProperty("user.dir")+"/cifar-10/train");
       
       File[] images=filee.listFiles();
       
       KafkaProducer<String,byte[]> producer=new KafkaProducer<String, byte[]>(properties);
       
       
       int sentfile=0;
       System.out.println("sending in  process.................");
   	for(File img:images) {
   		 int filenint=Integer.parseInt(img.getName().substring(0,img.getName().length()-4));
   		
   		    if(filenint<startreq1 || filenint>startreq2)continue;
   		  
		        byte[] filebyte;
		       
		        final int[] a=new int[1];
				try {
					
					filebyte = Files.readAllBytes(img.toPath());
					ProducerRecord<String,byte[]> record=new ProducerRecord<String,byte[]>(topicc,3,img.getName(),filebyte);
   		        
   		        producer.send(record,new Callback() {
   		
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							// TODO Auto-generated method stub
							if(exception==null) {
								a[0]=1;
								System.out.println("done in sending message");
							}else {
								
								System.out.println("error in sending message");
							}
							
						}
			        	
			        });
   		        
   		       
				} catch (IOException e) {
					e.printStackTrace();
				} 
				sentfile+=a[0];
		}
   	
   	System.out.println("Total of "+sentfile +" files are sent to "+topicc);
   	
   	
   	
   	
}
    
}
