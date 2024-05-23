package dstn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
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
import java.util.Random;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Node_2 implements Datapre{
	public static int add=0;
	public static  volatile Scanner scr;
	static String recieveFolder="node2_prime";
	static String recievedFolder="node1_prime";
	static String send="";
//	static String bootstrap_server="10.70.13.126:9092";
    static String topic ="node2";
    static String group_id="1";
    static int i=0;
    static int[] alive=new int[4];
    static File metafile1,metafile2;
    static FileWriter filewriter1,filewriter2;
   
    static HashMap<String,String> metadata1=new HashMap<String,String>();
    static HashMap<String,String> metadata2=new HashMap<String,String>();
    
    static ArrayList<String> nodesdata=new ArrayList<>(Arrays.asList(new String[] {"node1","node2","node3"}));
    
	   //0-----commands
	   //1-----recieving data
	   //2-----sending data
	   //3-----not decided

    static  KafkaProducer<String,byte[]> producerbytes;
    public static void main( String[] args ) throws IOException
    {
    	Properties properties1=new Properties();
        properties1.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties1.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties1.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
       
        
        producerbytes=new KafkaProducer<String, byte[]>(properties1);
           Initializemetadata();
           StartTheThreads();
		   try {
				Thread.sleep(1000);
		   } catch (InterruptedException e) {
				e.printStackTrace();
		   }
	       System.out.println(topic+" ...started");

    }
    
    
    
    
    
    
    private static void StartTheThreads() {
//    	
//    	Thread rancheck=new Thread(new Runnable() {
//	    	  public void run() {
//	    		  while(true) {
//	    			  System.out.println(Arrays.toString(alive));
//	    			  try {
//						Thread.sleep(5000);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//	    		  }
//	    	  }
//	      });
//    	rancheck.start();
    	
    	Thread sendheartbeat=new Thread(new Runnable() {
    		public void run() {
    			while(true) {
	    			sendping("node1",4);
	    			sendping("node3",5);
	    			sendping("mldata",5);
	    			try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			}
    		}
    	});
    	sendheartbeat.start();
    	
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
				 	                TopicPartition topicPartition = new TopicPartition(topic, 5);
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
				check3.start();
		    	
		    
		    	
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

    	Thread reciverdatafromml=new Thread(new Runnable() {
    		public void run() {
    			 Properties properties=new Properties();
	     	        
	     	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
	     	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
	     	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
	     	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
	     	        
	     		   KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String,byte[]>(properties);
	               TopicPartition topicPartition = new TopicPartition(topic, 2);
	               consumer.assign(Collections.singletonList(topicPartition));
	             
	              int turn=0;
	              while(true) {
	             	
	             	ConsumerRecords<String, byte[]> records=consumer.poll(Duration.ofMillis(100));
	             	
	                int createdfiles=0;
	             	for(ConsumerRecord<String,byte[]> record:records) {
	             		
	             		
	             		
	             		if(turn==0) {
	             			byte[] file_data=record.value();
		             		try (FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/"+record.key())) {
		             		    fos.write(file_data); // Write the byte array to the file
		             		    createdfiles++;
		             		   if(metadata1.containsKey(record.key().substring(0,record.key().length()-4)))continue;
		             		    metadata1.put(record.key().substring(0,record.key().length()-4), System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/"+record.key());
		             		    filewriter1.append(""+record.key().substring(0,record.key().length()-4)+" "+System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/"+record.key()+"\n");
		             		    filewriter1.flush();
		             		   
		             		} catch (IOException e) {
		             		    e.printStackTrace();
		             		    System.out.println(record.key()+"    "+Arrays.toString(file_data));
		             		}
		             		sendermaindata_cloning("node3",record);
	             		}else if(turn==1) {
	             			sendermaindata("node1",record);
	             			sendermaindata_cloning("node2",record);
	             			
	             		}else if(turn==2) {
	             			sendermaindata("node3",record);
	             			sendermaindata_cloning("node1",record);
	             		}
	             		
	             		turn=(turn+1)%3;
	             	}
	             		
	              }
	             
    		}
    	 });
		
    	 Thread recieverm=new Thread(new Runnable() {
	        	
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
	             		try (FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/"+record.key())) {
	             		    fos.write(file_data); // Write the byte array to the file
	             		    createdfiles++;
	             		   if(metadata1.containsKey(record.key().substring(0,record.key().length()-4)))continue;
	             		    metadata1.put(record.key().substring(0,record.key().length()-4), System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/"+record.key());
	             		   
	             		    filewriter1.append(""+record.key().substring(0,record.key().length()-4)+" "+System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/"+record.key()+"\n");
	             		    filewriter1.flush();
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
    	 Thread recieverr=new Thread(new Runnable() {
	        	
	        	public void run() {
	        		 Properties properties=new Properties();
	     	        
	     	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
	     	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
	     	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
	     	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
	     	        
	     		   KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String,byte[]>(properties);
	               TopicPartition topicPartition = new TopicPartition(topic, 3);
	               consumer.assign(Collections.singletonList(topicPartition));
	             
	             
	              while(true) {
	             	
	             	ConsumerRecords<String, byte[]> records=consumer.poll(Duration.ofMillis(100));
	             	
	                int createdfiles=0;
	             	for(ConsumerRecord<String,byte[]> record:records) {
	             		
	             		byte[] file_data=record.value();
	             		try (FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir")+"/cifar-10/node2/"+recievedFolder+"/"+record.key())) {
	             		    fos.write(file_data); // Write the byte array to the file
	             		    createdfiles++;
	             		   if(metadata2.containsKey(record.key().substring(0,record.key().length()-4)))continue;
	             		    metadata2.put(record.key().substring(0,record.key().length()-4), System.getProperty("user.dir")+"/cifar-10/node2/"+recievedFolder+"/"+record.key());
	             		   
	             		    filewriter2.append(""+record.key().substring(0,record.key().length()-4)+" "+System.getProperty("user.dir")+"/cifar-10/node2/"+recievedFolder+"/"+record.key()+"\n");
	             		    filewriter2.flush();
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
	        
	        
	  
			//recieving commands
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
					             		int startreq1=Integer.parseInt(str[1]);
					             		int startreq2=0;
					             		if(str[0].equals("send")) {
						             		System.out.println("sending initiated..................");
						             		sender(rcvdcommand,startreq1,startreq2);
						             		System.out.println("sending done..................");
					             		}else if(str[0].equals("sendfault")) {
					             			System.out.println("sending initiated..................");
					             			senderNode2fault(rcvdcommand,startreq1,startreq2);
						             		System.out.println("sending done..................");
					             		}
			             		
			             			 
			             			 
				
		                       	
			     
			             	}
			 		}
			 		
			 		
			 	}
			 });
			
			reciverdatafromml.start();
			 reccommands.start();
			 recieverr.start();   //node 3  clone
			 recieverm.start();   //main 
		
	}



     public static void sendping(String ttopic,int partition) {
		
		Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       
        
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
       
        final String topicname=ttopic;
        ProducerRecord<String,String> record=new ProducerRecord<String,String>(ttopic,partition,"key","alive");
    		producer.send(record,new Callback() {
    		        public void onCompletion(RecordMetadata metadata, Exception exception) {
							if(exception==null) {
								
							}else {
								System.out.println("message for sending error in sending message");
							}
						}
			        	
		    });
      }



	//for initializing metadata
    private static void Initializemetadata() throws IOException{
	
    	
    	 File folder=new File(System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder);
         folder.createNewFile();
         File folder1=new File(System.getProperty("user.dir")+"/cifar-10/node2/"+recievedFolder);
         folder1.createNewFile();
         
         metafile1=new File(System.getProperty("user.dir")+"/cifar-10/node2/"+recieveFolder+"/metafile1.txt");
         metafile1.createNewFile();
         metafile2=new File(System.getProperty("user.dir")+"/cifar-10/node2/"+recievedFolder+"/metafile1.txt");
         metafile2.createNewFile();
         
         filewriter1=new FileWriter(metafile1,true);
         filewriter2=new FileWriter(metafile2,true);
         
         FileReader fr=new FileReader(metafile1);
         FileReader fr1=new FileReader(metafile2);
         BufferedReader bf=new BufferedReader(fr);
         BufferedReader bf1=new BufferedReader(fr1);
         String str="";
         while((str=bf.readLine())!=null) {
         	String[] pair=str.split(" ");
         	metadata1.put(pair[0], pair[1]);
         }
         while((str=bf1.readLine())!=null) {
          	String[] pair=str.split(" ");
          	metadata2.put(pair[0], pair[1]);
          }
       System.out.println("Meta files are updated..............done");
	}







	//For sending the commands
    
    public static void sendcommand(String ttopic,String command) {
    	
    	                System.out.println("faulty inside"+ttopic+" "+command);
		
		        		Properties properties=new Properties();
		    	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
		    	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		    	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		    	       
		    	        
		    	        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
		    	       
		    	        final String topicname=ttopic;
		    	        ProducerRecord<String,String> record=new ProducerRecord<String,String>(ttopic,0,"key",command);
			        		producer.send(record,new Callback() {
			        		        public void onCompletion(RecordMetadata metadata, Exception exception) {
			    							if(exception==null) {
			    								System.out.println("Command sent successfully to "+topicname);
			    							}else {
			    								System.out.println("message for sending error in sending message");
			    							}
			    						}
			    			        	
			    		    });
	}
		    	      
		 
    
    
    public static void sendermaindata(String node,ConsumerRecord<String,byte[]> record) {
    	
        ProducerRecord<String,byte[]> datatobesent=new ProducerRecord<String,byte[]>(node,1,record.key(),record.value());
        final String ns=node;
        producerbytes.send(datatobesent,new Callback() {
    		
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if(exception==null) {
					System.out.println("data sent to "+ ns);
				}else {
					
					System.out.println("error in sending message");
				}
				
			}
        	
        });
    	
    }
    
    public static void sendermaindata_cloning(String node,ConsumerRecord<String,byte[]> record) {
    	
        ProducerRecord<String,byte[]> datatobesent=new ProducerRecord<String,byte[]>(node,3,record.key(),record.value());
        final String ns=node;
        producerbytes.send(datatobesent,new Callback() {
    		
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if(exception==null) {
					System.out.println("data sent to "+ ns);
				}else {
					
					System.out.println("error in sending message");
				}
				
			}
        	
        });
    	
    }
    
  //For Sending the message
    
    public static void sender(String command,int startreq1,int startreq2) {
     		
     		Properties properties=new Properties();
 	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
 	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
 	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
 	       
 	        
 	        KafkaProducer<String,byte[]> producer=new KafkaProducer<String, byte[]>(properties);
 	        
 	        int howmuch=startreq1/3;
 	       
 	       
 	        int sentfile=0;
 	        System.out.println("sending in  process................."+howmuch);
 	        Random r=new Random();
 	        HashSet<Integer> alreadytook=new HashSet<Integer>();
 	        List<String> ls=new ArrayList<>(metadata1.keySet());
         	while(howmuch>0) {
         		   
         		    int index=r.nextInt(metadata1.size());
         		    
         		   if(alreadytook.contains(index))continue;
         		   alreadytook.add(index);
         		    String imgname=metadata1.get(ls.get(index));
         		    
         		   
         		    File file=new File(imgname);
     		        byte[] filebyte;
     		        
     		        final int[] a=new int[1];
 					try {
 						
 						filebyte = Files.readAllBytes(file.toPath());
 						ProducerRecord<String,byte[]> record=new ProducerRecord<String,byte[]>("mldata",1,file.getName(),filebyte);
         		        
         		        producer.send(record,new Callback() {
         		
     						public void onCompletion(RecordMetadata metadata, Exception exception) {
     							// TODO Auto-generated method stub
     							if(exception==null) {
     								a[0]=1;
     							}else {
     								
     								System.out.println("error in sending message");
     							}
     							
     						}
     			        	
     			        });
         		        
         		       
 					} catch (IOException e) {
 						e.printStackTrace();
 					} 
 					sentfile+=a[0];
 					howmuch--;
     		}
         	
         	System.out.println("Total of "+sentfile +" files are sent");
         	
         	
         	
        	String[] str=command.split(" ");
           	HashSet<String> ignore=new HashSet<>();
           
           	if(str.length>3) {
           		for(int i=3;i<str.length;i++) {
           			ignore.add(str[i]);
           		}
           	}
        	System.out.println(ignore);
           	System.out.println("command is "+command +" and ignore is "+ignore);
           	String ncommand=command+" "+"node1 node2 node3";
           	
           	
           	for(int z=0;z<nodesdata.size();z++) {
		   	    String nodes=nodesdata.get(z);
		   		if(!nodes.equals(topic) && !ignore.contains(nodes) && alive[z+1]==1) {
		   			sendcommand(nodes,ncommand);
		   		}else if(!nodes.equals(topic) && !ignore.contains(nodes) && alive[z+1]==0) {
		   			//do the fault tolerance part
		   			if(z==0) {
		   				sendcommand("node2","sendfault"+ncommand.substring(4,ncommand.length()));
		   			}else if(z==2 && alive[1]==1) {
		   				sendcommand("node1","sendfault"+ncommand.substring(4,ncommand.length()));
		   			}
		       }
		   	
       }
    }
   
   public static void senderNode2fault(String command,int startreq1,int startreq2) {
		
		Properties properties=new Properties();
       properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
       properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      
       
       KafkaProducer<String,byte[]> producer=new KafkaProducer<String, byte[]>(properties);
       
       int howmuch=startreq1/3;
       int rem=startreq1-(howmuch*3);
       howmuch+=rem;
      
       int sentfile=0;
       System.out.println("sending in  process................."+howmuch);
       Random r=new Random();
       HashSet<Integer> alreadytook=new HashSet<Integer>();
       List<String> ls=new ArrayList<>(metadata2.keySet());
    	while(howmuch>0) {
   		   
   		    int index=r.nextInt(metadata2.size());
   		    
   		   if(alreadytook.contains(index))continue;
   		   alreadytook.add(index);
   		    String imgname=metadata2.get(ls.get(index));
   		    
   		   
   		    File file=new File(imgname);
		        byte[] filebyte;
		        
		        final int[] a=new int[1];
				try {
					
					filebyte = Files.readAllBytes(file.toPath());
					ProducerRecord<String,byte[]> record=new ProducerRecord<String,byte[]>("mldata",1,file.getName(),filebyte);
   		        
   		           producer.send(record,new Callback() {
   		
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							// TODO Auto-generated method stub
							if(exception==null) {
								a[0]=1;
							}else {
								
								System.out.println("error in sending message");
							}
							
						}
			        	
			        });
   		        
   		       
				} catch (IOException e) {
					e.printStackTrace();
				} 
				sentfile+=a[0];
				howmuch--;
		}
   	
   	System.out.println("Total of "+sentfile +" files are sent For the faulty node");
   	
   	
   	
   
}

    
}
