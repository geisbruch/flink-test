package com.ml.test;

import java.util.Date;

public class TestDto {
    String id;
    Integer amount;
    Date time;

    public TestDto(){}

    public TestDto(String id, Integer amount, Date time) {
        this.id = id;
        this.amount = amount;
        this.time = time;

    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Date getTime() {
        return time;
    }

    public Integer getAmount() {
        return amount;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "id="+id+",amount="+amount;
    }
}
