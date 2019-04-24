package dubstep;
import net.sf.jsqlparser.expression.*;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

public class TupleReader implements RowTraverser
{

    DataInputStream reader;
    String FileName;
    FieldType[] fieldTypes;
    HashMap<String,Integer> fieldMapping;

    public TupleReader(String fileName, FieldType[] fieldTypes,HashMap<String,Integer> fieldMapping) throws IOException
    {
        this.FileName = fileName;
        this.reader = new DataInputStream(new BufferedInputStream(new FileInputStream(FileName)));
        this.fieldTypes = fieldTypes;
        this.fieldMapping = fieldMapping;
    }

    public TupleReader(String TableName, HashMap<String, Integer> fieldMapping)throws IOException
    {
        this.FileName = Table.getDirectory()+ TableName+ ".dat";
        this.reader = new DataInputStream(new BufferedInputStream(new FileInputStream(FileName)));
        this.fieldMapping = fieldMapping;
        this.fieldTypes = TableInformation.getFieldTypes(TableName);
    }

    @Override
    public PrimitiveValue[] next()
    {
        PrimitiveValue[] tuple = reader!=null ? readerTuple(): null;
        return tuple;
    }

    private PrimitiveValue[] readerTuple()
    {
        try
        {
            PrimitiveValue[] tuple = new PrimitiveValue[fieldTypes.length];

            for (int i=0; i < fieldTypes.length; i++)
            {
                switch (fieldTypes[i])
                {
                    case DATE: tuple[i] = new DateValue(reader.readUTF()); break;
                    case STRING: tuple[i] = new StringValue(reader.readUTF()); break;
                    case INT: tuple[i] = new LongValue(reader.readLong()); break;
                    case DOUBLE: tuple[i] = new DoubleValue(reader.readDouble()); break;

                }
            }

            return tuple;

        }
        catch (Exception e)
        {
            return null;
        }

    }

    @Override
    public void reset() throws IOException
    {
        this.reader = new DataInputStream(new BufferedInputStream(new FileInputStream(FileName)));
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return fieldMapping;
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    @Override
    public PrimitiveValue[] getcurrent()
    {
        return new PrimitiveValue[0];
    }
}
