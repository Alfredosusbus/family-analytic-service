package com.familybudget.analytic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
@RequestMapping("/analytic")
public class AnalyticServiceApplication {

    @GetMapping("/hello")
    public String hello() {
        return "Hello from Analytic Service!";
    }

    public static void main(String[] args) {
        SpringApplication.run(AnalyticServiceApplication.class, args);
    }
}