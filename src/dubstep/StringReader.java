package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class StringReader implements DataReader
{
    String FileName;
    DataInputStream reader;


    public StringReader(String fileName) throws IOException
    {
        this.FileName = fileName;
        this.reader = new DataInputStream(new BufferedInputStream(new FileInputStream(FileName)));
    }

    @Override
    public PrimitiveValue readData() throws IOException
    {
        return new StringValue(reader.readUTF());
    }



    @Override
    public void reset() throws IOException
    {
        this.reader = new DataInputStream(new BufferedInputStream(new FileInputStream(FileName)));

    }


    @Override
    public void close() throws IOException, ClassNotFoundException
    {
        reader.close();
    }


}
