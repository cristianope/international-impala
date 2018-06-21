package com.app99.international.model;



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

    public String toParquet(){
        return " CAST(" + getField() + " as " + getType() + ") ";
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