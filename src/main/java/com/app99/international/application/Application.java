package com.app99.international.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.app99.international")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}