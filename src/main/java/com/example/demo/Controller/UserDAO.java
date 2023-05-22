package com.example.demo.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpRequest;
import org.springframework.stereotype.Repository;
import org.springframework.web.multipart.MultipartFile;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.example.demo.bean.User;

@Repository
public class UserDAO {
	public static String connectionString = "";
	static {
		try(InputStream input=UserDAO.class.getClassLoader().getResourceAsStream("config.properties")){
			Properties p=new Properties(); 
			if(input==null) {
				System.out.println("could not find config file");
			}
			  p.load(input);
			  connectionString=p.getProperty("connectionString");
			}
		    catch (FileNotFoundException e) {
		          e.printStackTrace(); 
		    } 
		    catch (IOException e1) {
              e1.printStackTrace(); 
		  }
	}

	public void createTable() {
		try{
			TableServiceClient tableServiceClient = new TableServiceClientBuilder().connectionString(UserDAO.connectionString).buildClient();
		    TableClient tableClient = tableServiceClient.createTableIfNotExists("Users");
		    System.out.println("Table created !");
		}
		catch(Exception e) {
			System.out.println("Table could not be created !");
		}
	}

	public void insertEntity(User user, MultipartFile file) {
		this.createTable();
		TableClient tableClient = new TableClientBuilder().connectionString(connectionString).tableName("Users").buildClient();
		
		String partitionKey = "Users";
	    String rowKey = user.getEmail();
		Map<String, Object> personalInfo= new HashMap<>();
	    personalInfo.put("Name", user.getName());
	    personalInfo.put("Email", user.getEmail());
	    personalInfo.put("Gender", user.getGender());
	    personalInfo.put("ContactNumber", user.getContact());
	    TableEntity employee = new TableEntity(partitionKey,rowKey).setProperties(personalInfo);
	        
	    
	    BlobContainerClient blobContainer=new BlobContainerClientBuilder().connectionString(connectionString).containerName("blob-container").
	    		buildClient();
	    BlobClient blobClient=blobContainer.getBlobClient(user.getEmail()+".jpg");
	    try {
	    	if(file.getSize()!=0)
			   blobClient.upload(file.getInputStream(), file.getSize(), true);
	    	else {
	    		String dir=new ClassPathResource("static/image/").getFile().getAbsolutePath();
		    	String path=dir+File.separator+"default.jpg";
		    	FileInputStream fis=new FileInputStream(path);
		    	File default_pic=new File(path);
		    	blobClient.upload(fis, default_pic.length(), true);
		    	default_pic.delete();
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("blob not stored");
			e.printStackTrace();
		}
	    
	    // Upsert the entity into the table
	    tableClient.upsertEntity(employee);
		System.out.println("User registered !");
	}

	public List<User> getEntities() {
		List<User> users =new ArrayList<>();
		TableClient tableClient = new TableClientBuilder().connectionString(connectionString).tableName("Users").buildClient();
		 ListEntitiesOptions options = new ListEntitiesOptions().setFilter("PartitionKey eq 'Users'");

		    // Loop through the results, displaying information about the entities.
		    tableClient.listEntities(options, null, null).forEach(tableEntity -> {
		        User user=new User();
		        user.setName((String)tableEntity.getProperty("Name"));
		        user.setEmail((String)tableEntity.getProperty("Email"));
		        user.setGender((String)tableEntity.getProperty("Gender"));
		        user.setContact((String)tableEntity.getProperty("ContactNumber"));
		        users.add(user);
		    });
			return users;
		
	}

	public User getUser(String email) {
		TableClient tableClient = new TableClientBuilder().connectionString(connectionString).tableName("Users").buildClient();
		TableEntity entity = tableClient.getEntity("Users", email);
		User user=new User();
		if(entity != null) {
			user.setName((String)entity.getProperty("Name"));
			user.setEmail((String)entity.getProperty("Email"));
	        user.setGender((String)entity.getProperty("Gender"));
	        user.setContact((String)entity.getProperty("ContactNumber"));
		}
		return user;
	}
	
}
