package com.app99.international.model;

import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ReadFile {

    public List<String> getFile(String fileName) {
        List<String> result = new ArrayList<String>();
        try{
            BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/" + fileName)));
            String line;
            while ((line = br.readLine()) != null) {
                result.add(line);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
