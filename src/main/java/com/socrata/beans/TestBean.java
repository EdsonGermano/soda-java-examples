package com.socrata.beans;

import java.util.Date;

/**
 * A test bean
 */
public class TestBean
{
    String name;
    String value;
    Integer count;
    Date    date;

    public TestBean()
    {
    }

    public TestBean(String name, String value, Integer count, Date date)
    {
        this.name = name;
        this.value = value;
        this.count = count;
        this.date = date;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public Integer getCount()
    {
        return count;
    }

    public void setCount(Integer count)
    {
        this.count = count;
    }

    public Date getDate()
    {
        return date;
    }

    public void setDate(Date date)
    {
        this.date = date;
    }
}
