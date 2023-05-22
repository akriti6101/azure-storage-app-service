package com.example.demo.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.example.demo.bean.User;


@Controller
public class MyController {
    @Autowired
	private UserService userService;
	
	@RequestMapping("/")
	public String home() {
		return "home";
	}
	@RequestMapping("/register")
	public String registrationPage() {
		return "Register";
	}
	@PostMapping("/registerData")
	public String registerData(@ModelAttribute("user") User user, @RequestParam("file") MultipartFile file,
			HttpServletRequest req) throws IOException {
		userService.registerData(user,file);
		return "home";
	}
	@RequestMapping("/showData")
	public String showData(Model model) {
		List<User> users=userService.showData();
		model.addAttribute("users", users);
		return "showUsers";
	}
	@GetMapping("/searchProfile")
	public String searchProfile(Model model) {
		List<User> users=userService.showData();
	    model.addAttribute("users", users);
		return "viewUser";
	}
	
	@PostMapping("/userProfile")
	public String showProfile(@RequestParam("email") String email,Model model) {
		User user=userService.showProfile(email);
	    model.addAttribute("user", user);
		return "profile";
	}
}
