package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class File_Value implements Writable {

    Text file;
    IntWritable value;

    public File_Value(String file, Integer value) {
        this.file = new Text(file);
        this.value = new IntWritable(value);
    }

    public File_Value() {
        this.file = new Text();
        this.value = new IntWritable();
    }

    public String getFile() {
        return file.toString();
    }

    public int getValue() {
        return value.get();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file.readFields(in);
        value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        file.write(out);
        value.write(out);
    }

}
