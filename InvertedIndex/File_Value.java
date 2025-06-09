package it.unipi.hadoop.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class File_Value implements Writable{
    String file;
    Integer value;

    public File_Value(String file, Integer value){
        this.file=file;
        this.value=value;
    }

    public File_Value(){}

    public void readFields(DataInput in) throws IOException
    {
        this.file = in.readUTF();
        this.value = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(this.file);
        out.writeInt(this.value);
    }
}
