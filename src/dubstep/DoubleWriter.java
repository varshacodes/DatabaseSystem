package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class DoubleWriter implements DataWriter
{
    DataOutputStream writer;
    String FileName;

    public DoubleWriter(String FileName)throws IOException
    {
        this.FileName = FileName;
        this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(FileName)));

    }

    @Override
    public void writeData(String data) throws IOException
    {
        writer.writeDouble(new Double(data));

    }

    @Override
    public void writeData(PrimitiveValue data)throws IOException, PrimitiveValue.InvalidPrimitive
    {
        writer.writeDouble(data.toDouble());
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
    }
}
