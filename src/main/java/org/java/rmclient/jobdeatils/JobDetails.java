package org.java.rmclient.jobdeatils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JobDetails {
	YarnConfiguration conf;
	YarnClient yarnClient;
	SimpleDateFormat sdf;
	Set<String> set_app_type;
	String app_type;
	JSONObject obj;
	JSONArray arrData;
	String ClusterName;
	
	public JobDetails(String s){
		this.ClusterName=s;
		conf = new YarnConfiguration();
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
        yarnClient.start();
        sdf = new SimpleDateFormat();
	    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
	    
	    set_app_type = new HashSet();
    	set_app_type.add("TEZ");
    	set_app_type.add("SPARK");
    	set_app_type.add("MAPREDUCE");
    	
    	obj = new JSONObject();
    	arrData = new JSONArray();
	}
	
	public String getRunningJobs(){
        String s=new String();
        int count_jobs=0;
       /* Resource manager related details */ 
        try { 	
        	//For application state
        	EnumSet<YarnApplicationState> appStates = EnumSet.of(YarnApplicationState.RUNNING);
            List<ApplicationReport> applications = yarnClient.getApplications(set_app_type,appStates);
   
            s=s+"<html><body><h3>Count of total running jobs on cluster: "+ applications.size() +"</h3><h3>Long running jobs details On GCIA and CKPPPROD Queues:</h3><ul><li>Running jobs threshold : Jobs running for more than 30 mins.</li></ul>"
            		+ "<table style='border:2px solid black'>";
            //s=s+"<tr style='border:2px solid black'><td style='border:2px solid black'><b>USERNAME</td><td style='border:2px solid black'><b>APPLICATION ID</td><td style='border:2px solid black'><b>JOB START TIME (GMT)</td><td style='border:2px solid black'><b>JOB RUN DURATION (HH:MM:SS)</td><td style='border:2px solid black'><b>QueueName</td><td style='border:2px solid black'><b>Application Type</td><td style='border:2px solid black'><b>JOB Progress</td></tr>";
            s=s+"<tr style='border:2px solid black'><td style='border:2px solid black'><b>USERNAME</td><td style='border:2px solid black'><b>APPLICATION ID</td><td style='border:2px solid black'><b>JOB START TIME (GMT)</td><td style='border:2px solid black'><b>JOB RUN DURATION (HH:MM:SS)</td><td style='border:2px solid black'><b>QueueName</td><td style='border:2px solid black'><b>Application Type</td><td style='border:2px solid black'><b>Application State</td></tr>";

            for(ApplicationReport app:applications){
            	
            	long millisCurrentTime = System.currentTimeMillis() ;
            	long job_start_time=app.getStartTime();
            	
                long job_run_duration_millis = millisCurrentTime-job_start_time;

                // if job is in running state for more than 30 min
                //System.out.println(app.getQueue() + "Time ::" + job_run_duration_millis);
                if(job_run_duration_millis>1800000){
                	
                	String app_id=app.getApplicationId().getClusterTimestamp() + "_" + app.getApplicationId().getId();
                	//System.out.println(app.getQueue() + " JobId ::" + app_id);
                	if(app.getName().startsWith("oozie:") ){
                		app_type="OOZIE/MAPREDUCE";
                		if(job_run_duration_millis<1800000){
                			continue;
                		}
                	}
                	else{
                		app_type=app.getApplicationType();
                	}
                	count_jobs++;
                	s=s+"<tr style='border:2px solid black'>";
                	s=s+"<td style='border:2px solid black'>"+app.getUser()+"</td>";
                	s=s+"<td style='border:2px solid black'>"+app_id+"</td>";
                	s=s+"<td style='border:2px solid black'>"+sdf.format(new Date(app.getStartTime()))+"</td>";
                	s=s+"<td style='border:2px solid black'>"+String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(job_run_duration_millis),
	    	                TimeUnit.MILLISECONDS.toMinutes(job_run_duration_millis) % TimeUnit.HOURS.toMinutes(1),
	    	                TimeUnit.MILLISECONDS.toSeconds(job_run_duration_millis) % TimeUnit.MINUTES.toSeconds(1))+"</td>";
                	s=s+"<td style='border:2px solid black'>"+app.getQueue()+"</td>";
                	s=s+"<td style='border:2px solid black'>"+app_type+"</td>";
                	s=s+"<td style='border:2px solid black'>"+"Running"+"</td>";
                	//s=s+"<td style='border:2px solid black'>"+app.getProgress()+"</td>";
                	s=s+"</tr>";     
                    
                	/* getting json file */
                	/*
                	JSONObject temp = new JSONObject();
                	temp.put("job_runtime",String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(job_run_duration_millis),
	    	                TimeUnit.MILLISECONDS.toMinutes(job_run_duration_millis) % TimeUnit.HOURS.toMinutes(1),
	    	                TimeUnit.MILLISECONDS.toSeconds(job_run_duration_millis) % TimeUnit.MINUTES.toSeconds(1)));
                	temp.put("user_name",app.getUser());
                	temp.put("job_type",app_type);
                	temp.put("job_starttime",sdf.format(new Date(app.getStartTime())));
                	temp.put("job_queue",app.getQueue());
                	temp.put("job_id",app_id);
                	arrData.add(temp);
                	*/
                }
            }
			
            //If there are no running jobs for the given threshold then put empty objects in the file
            if(count_jobs==0){
            	//JSONObject temp =null;
            	//arrData.add(temp);
            	System.out.println("No running jobs in given Thresold");
            	s="";
            }
            /* writing data to file */
            /*
            obj.put("ClusterName", arrData);
            obj.put("TotalRunningJobCount", count_jobs);
            String fileName="/u/spool/30/" + ClusterName +"_running_datafile.json";
            System.out.println("Writing to file");
            FileWriter file = new FileWriter(fileName);
            file.write(obj.toJSONString());
    	    System.out.println("Successfully Copied JSON Object to File...");
    	    file.flush();
    		file.close();
    		*/
    		
        } //end of try block
        catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
		return s;
    }//end of method getRunningJobs
    
    public String getPendingJobs(){
        String s=new String();
        int count_jobs=0;
       /* Resource manager related details */ 
        try { 	
        	
        	//For application state
        	EnumSet<YarnApplicationState> appStates = EnumSet.of(YarnApplicationState.ACCEPTED);
            List<ApplicationReport> applications = yarnClient.getApplications(set_app_type,appStates);
      
            s=s+"<html><body><h3>Count of total jobs in ACCEPTED cluster: "+ applications.size() +"</h3><h3>Long running jobs details On GCIA and CKPPPROD Queue:</h3><ul><li>Jobs threshold : Jobs in ACCETED state for more than 2 minutes.</li></ul>"
            		+ "<table style='border:2px solid black'>";
            s=s+"<tr style='border:2px solid black'><td style='border:2px solid black'><b>USERNAME</td><td style='border:2px solid black'><b>APPLICATION ID</td><td style='border:2px solid black'><b>JOB SUBMIT TIME (GMT)</td><td style='border:2px solid black'><b>JOB PENDING DURATION (HH:MM:SS)</td><td style='border:2px solid black'><b>QueueName</td><td style='border:2px solid black'><b>Application Type</td><td style='border:2px solid black'><b>Application State</td></tr>";
            
            String app_id=null;
            
            for(ApplicationReport app:applications){
            	long millisCurrentTime = System.currentTimeMillis() ;
            	long job_start_time=app.getStartTime();
            	
                long job_pending_duration_millis = millisCurrentTime-job_start_time;
                //job_pending_duration_millis>300000
                // if job is in accepted state for more than 2min
                if(job_pending_duration_millis>120000 && (app.getQueue().equals("root.gcia") || app.getQueue().equals("root.ckppprod")))
                {
                	app_id=app.getApplicationId().getClusterTimestamp() + "_" + app.getApplicationId().getId();
                	app_type=app.getApplicationType();
                	if(app.getName().startsWith("oozie:") ){
                		app_type="OOZIE/MAPREDUCE";
                	}
                	count_jobs++;
                	s=s+"<tr style='border:2px solid black'>";
                	s=s+"<td style='border:2px solid black'>"+app.getUser()+"</td>";
                	s=s+"<td style='border:2px solid black'>"+app_id+"</td>";
                	s=s+"<td style='border:2px solid black'>"+sdf.format(new Date(app.getStartTime()))+"</td>";
                	s=s+"<td style='border:2px solid black'>"+String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(job_pending_duration_millis),
	    	                TimeUnit.MILLISECONDS.toMinutes(job_pending_duration_millis) % TimeUnit.HOURS.toMinutes(1),
	    	                TimeUnit.MILLISECONDS.toSeconds(job_pending_duration_millis) % TimeUnit.MINUTES.toSeconds(1))+"</td>";
                	s=s+"<td style='border:2px solid black'>"+app.getQueue()+"</td>";
                	s=s+"<td style='border:2px solid black'>"+app_type+"</td>";
                	s=s+"<td style='border:2px solid black'>"+"Accepted"+"</td>";
                	//s=s+"<td style='border:2px solid black'>"+app.getProgress()+"</td>";
                	s=s+"</tr>";      	
                }
                //System.err.println("Jobid : " + app.getApplicationId().getClusterTimestamp() + "_" + app.getApplicationId().getId() + "\t Username : " + app.getUser());
            }

            if(count_jobs==0){
            	System.out.println("No pending jobs in given Thresold");
            	s="";
            }
        }
        catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
		return s;
    }
}
