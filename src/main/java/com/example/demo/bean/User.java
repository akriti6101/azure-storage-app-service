package com.example.demo.bean;

public class User {
   private String name;
   private String email;
   private String gender;
   private String contact;
   
public User() {
	super();
}
public User(String name, String email, String gender, String contact) {
	super();
	this.name = name;
	this.email = email;
	this.gender = gender;
	this.contact = contact;
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public String getEmail() {
	return email;
}
public void setEmail(String email) {
	this.email = email;
}
public String getGender() {
	return gender;
}
public void setGender(String gender) {
	this.gender = gender;
}
public String getContact() {
	return contact;
}
public void setContact(String contact) {
	this.contact = contact;
}
@Override
public String toString() {
	return "name=" + name + ", email=" + email + ", gender=" + gender + ", contact=" + contact + "";
}
   
   
}
