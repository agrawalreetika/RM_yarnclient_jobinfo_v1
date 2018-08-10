package org.java.rmclient.jobdeatils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Author :: Reetika Agrawal (ragraw1)
 * 
 * getRunningJobs() Method :: This method is to get running jobs in a particular cluster.
 * Thresold :
 * Mapreduce - More than 1hr
 * Oozie - More than 8 hrs
 * 
 * getPendingJobs() Method :: This method is to get pending jobs in a particular cluster.
 * Thresold :
 * Alljobs - More than 5 min
 * 
 * getAllQueues() Method :: This method is to get all queues in a particular cluster.
 */


public class MainClient
{
	static YarnConfiguration conf = new YarnConfiguration();
	static YarnClient yarnClient = YarnClient.createYarnClient();
	
	static Properties prop = new Properties();
	static InputStream input = null;

    public static void main( String[] args )throws IOException, YarnException, AddressException, MessagingException, InterruptedException {
    	yarnClient.init(conf);
        yarnClient.start();

        input = new FileInputStream("config.properties");
        prop.load(input);
        
        String remoteServer = prop.getProperty("remoteServer");
        String remoteLocation = prop.getProperty("remoteLocation");
        
        String ClusterName=getClustername();
        
        /* For long running jobs */
        String longRunningJobsList=new JobDetails(ClusterName).getRunningJobs();
        String FileLocation="/u/spool/30/" + ClusterName +"_running_datafile_new.json";
        
        scpFileToServer(FileLocation,remoteServer,remoteLocation);
        /*
        if(!longRunningJobsList.equals("")){
        	if(args.length==0){
        		System.out.println("Not sending any mail,no mail id is given in the argument");
        	}
        	else{
            	MailSend obj2=new MailSend(longRunningJobsList,"Long Pending Jobs Details on ",args[0]);
            	obj2.sendEmail();
        	}
        }*/
        
        
        /* For long pending jobs */
        String longPendingJobsList=new JobDetails(ClusterName).getPendingJobs();
        /*
        if(!longPendingJobsList.equals("")){
        	if(args.length==0){
        		System.out.println("Not sending any mail,no mail id is given in the argument");
        	}
        	else{
            	MailSend obj2=new MailSend(longPendingJobsList,"Long Pending Jobs Details on ",args[0]);
            	obj2.sendEmail();
        	}
        }*/
        
        if(!longRunningJobsList.equals("") || !longPendingJobsList.equals("")){
        	if(args.length==0){
        		System.out.println("Not sending any mail,no mail id is given in the argument");
        	}
        	else{
        		String fullContent=longPendingJobsList+longRunningJobsList;
            	MailSend obj2=new MailSend(fullContent,"Long Running/Pending Jobs Details on ",args[0]);
            	obj2.sendEmail();
        	}
        }
        
        /* Sending a combined mail for running and pending jobs */
        
        //get all queues
        /*
        List<String> queues_list=getAllQueues();
        System.out.println("Count of queues on " + getClustername() + " is : " + queues_list.size());
        for(String l:queues_list){
        	System.out.println(l);
        	}
        */
    }
    
    /* This is to copy file to oser401442 through svchdpdb user id */
    public static void scpFileToServer(String fileLocation,String remoteServer,String remoteLocation) throws IOException, InterruptedException {
    	String cmd = "scp " + fileLocation +" " + remoteServer + ":" + remoteLocation;
    	Runtime run = Runtime.getRuntime();
    	Process pr = run.exec(cmd);
    	pr.waitFor();
	}

	public static String getClustername() throws IOException{
    	  FileSystem fs = FileSystem.get(conf);
          String clustername=fs.getUri().toString().replace("hdfs://", "");
          clustername=clustername.toUpperCase();
          if(clustername.contains("HA")){
          	clustername=clustername.replace("HA", "");
          }
		return clustername;  	
    }   
    
    public static List<String> getAllQueues() throws YarnException, IOException{
    	List<QueueInfo> applications = yarnClient.getAllQueues();
    	
    	List<String> queues=new ArrayList<String>();
    	for(QueueInfo app:applications){
    		queues.add(app.getQueueName());
    	}
    	return queues;
    }
    
 
}
