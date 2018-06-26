package com.app99.international.model;

import org.springframework.stereotype.Component;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Functions {

    public static boolean startJob(String dateStart) throws ParseException{
        SimpleDateFormat f = new SimpleDateFormat("dd-MM-yyyy");
        Date d = f.parse(dateStart);
        return (System.currentTimeMillis() >= d.getTime()) ;
    }

    public static List<String> getFile(String fileName) {
        List<String> result = new ArrayList<String>();
        try{

            BufferedReader br = new BufferedReader(new InputStreamReader(Functions.class.getResourceAsStream("/" + fileName)));
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
