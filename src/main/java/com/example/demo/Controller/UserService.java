package com.example.demo.Controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.example.demo.bean.User;

@Service
public class UserService {
    @Autowired
    private UserDAO userDAO;

	public UserService(UserDAO userDao) {
		super();
		this.userDAO = userDao;
	}

	public UserService() {
	} 
    
	public void registerData(User user, MultipartFile file) {
		this.userDAO.insertEntity(user,file);
	}
    
	public List<User> showData() {
		
		List<User> users=this.userDAO.getEntities(); 
		return users;
	}

	public User showProfile(String email) {
      User user=this.userDAO.getUser(email);
		return user;
	}
}
