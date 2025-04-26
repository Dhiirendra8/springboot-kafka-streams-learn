package com.learn.kstream.model;

import java.util.Date;

public class WordCount {
    private String key;
    private Long value;
    private Date date;
    private Date date1;

    public WordCount(String key, Long value, Date date, Date date1) {
        this.key = key;
        this.value = value;
        this.date = date;
        this.date1 = date1;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
