package com.app99.international.model;


import java.util.List;

public class Field {

    private String field;
    private String type;

    public Field(String field, String type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toCreateDDL(String tableName){
        return getField() + " " + changeType(tableName);
    }

    public String toParquet(String tableName){
        return " CAST(" + getField() + " AS " + changeType(tableName) + ") ";
    }

    private String changeType(String tableName) {
        List<String> linhas = new ReadFile().getFile("tables_columns_change");

        for (String linha : linhas){
            String[] columns = linha.split("=");
            String table = columns[0];

            if (tableName.equals(table)){
                String[] tuples = columns[1].split(",");

                for (String tuple: tuples) {
                    String[] field = tuple.split(":");

                    if (this.field.equals(field[0])){
                        return field[1];
                    }
                }
            }
        }
        return type;
    }

    @Override
    public String toString() {
        return field + " " + type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Field)) return false;

        Field field1 = (Field) o;

        return getField() != null ? getField().equals(field1.getField()) : field1.getField() == null;
    }

    @Override
    public int hashCode() {
        return getField() != null ? getField().hashCode() : 0;
    }
}