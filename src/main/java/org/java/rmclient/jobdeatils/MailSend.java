package org.java.rmclient.jobdeatils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MailSend {
	YarnConfiguration conf;
	Properties properties;
	Session session;
	MimeMessage message;
    String mailContent,mailSubject;
    String to;
    
    static Properties prop = new Properties();
	static InputStream input = null;
    
	public MailSend(String mailContent,String mailSubject,String mailTo){	
		this.mailContent=mailContent;
		this.mailSubject=mailSubject;
		this.to=mailTo;
		
		conf = new YarnConfiguration();
		// Get system properties
	    properties = System.getProperties();
		// Get the default Session object.
	    session = Session.getDefaultInstance(properties);
		// Create a default MimeMessage object.
	    message = new MimeMessage(session);
	}
	
	public void sendEmail() throws AddressException, MessagingException, IOException{
		input = new FileInputStream("config.properties");
        prop.load(input);
        
        // Sender's email ID needs to be mentioned
        String from = prop.getProperty("mailFrom");

        // Assuming you are sending email from localhost
        String host = prop.getProperty("mailhost");

        properties.setProperty("mail.smtp.host", host);

        // Set From: header field of the header.
        message.setFrom(new InternetAddress(from));

        // Set To: header field of the header.
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

        // Set Subject: header field
        message.setSubject(mailSubject + getClustername() + " - Long Running jobs");
        
        message.setContent(mailContent, "text/html");
        // Send message
        Transport.send(message);
        System.out.println("Sent message successfully....");
	}
	
	public String getClustername() throws IOException{
  	  FileSystem fs = FileSystem.get(conf);
        String clustername=fs.getUri().toString().replace("hdfs://", "");
        clustername=clustername.toUpperCase();
        if(clustername.contains("HA")){
        	clustername=clustername.replace("HA", "");
        }
		return clustername;  	
  }

}
