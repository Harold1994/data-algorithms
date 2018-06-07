package ch01.mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();


    public DateTemperaturePair() {
    }

    public DateTemperaturePair(String yearMonth, String day, int temperature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperature);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        yearMonth.write(dataOutput);
        day.write(dataOutput);
        temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }


    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    public void setTemperature(int Temp) {
        temperature.set(Temp);
    }

    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        return -1 * compareValue;//降序排序
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTemperaturePair that = (DateTemperaturePair) o;
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != yearMonth) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DateTemperaturePair{" +
                "yearMonth=" + yearMonth +
                ", day=" + day +
                ", temperature=" + temperature +
                '}';
    }
}

