package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class LongWriter implements DataWriter
{
    DataOutputStream writer;
    String FileName;



    public LongWriter(String FileName)throws IOException
    {
        this.FileName = FileName;
        this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(FileName)));

    }

    @Override
    public void writeData(String data) throws IOException
    {
        writer.writeLong(new Long(data));

    }

    @Override
    public void writeData(PrimitiveValue data)throws IOException, PrimitiveValue.InvalidPrimitive
    {
        writer.writeLong(data.toLong());
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
    }
}
