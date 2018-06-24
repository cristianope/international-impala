package com.app99.international.integration;


public interface DAO {

    default String verifyField(String field, String typeName){
        if(typeName.startsWith("character")){
            return "string";
        }else{
            if(typeName.startsWith("numeric")){
                return typeName.replace("numeric", "decimal");
            }else{
                if(typeName.startsWith("integer")){
                    return typeName.replace("integer","int");
                }
            }
        }

        if(field.equals("year")){
            return "smallint";
        }else{
            if(field.contains("month,day,hour")){
                return "tinyint";
            }
        }

        return typeName;

    }
}
