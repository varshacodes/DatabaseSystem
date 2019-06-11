package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;

public class TupleTraverser implements RowTraverser
{


    DataReader[] readers;
    String FolderName;
    FieldType[] fieldTypes;
    HashMap<String,Integer> fieldMapping;

    public TupleTraverser(String TableName, boolean hasTableAlias, String TableAlias)throws IOException
    {
        Table Table = TableInformation.getTable(TableName);
        this.FolderName = Table.getFolderName();
        this.fieldMapping = hasTableAlias ? Table.getFieldMappingwithAlias(TableAlias): Table.getFieldPostionMapping();
        this.fieldTypes = Table.getFieldTypes();
        setReaders();

    }
    @Override
    public int getNoOfFields()
    {
        return fieldTypes.length;
    }

    private void setReaders() throws IOException
    {
        readers = new DataReader[fieldTypes.length];

        for(int i=0; i < fieldTypes.length; i++)
        {
            String fileName = this.FolderName+ "Field-"+(i+1);

            switch (fieldTypes[i])
            {
                case DOUBLE: readers[i] = new DoubleReader(fileName); break;
                case INT: readers[i] = new LongReader(fileName); break;
                case DATE: readers[i] = new DateReader(fileName); break;
                case STRING: readers[i] = new StringReader(fileName); break;
            }

        }


    }


    @Override
    public PrimitiveValue[] next() throws IOException
    {
        try
        {
            PrimitiveValue[] datarow = new PrimitiveValue[fieldTypes.length];

            for(int i=0; i < datarow.length; i++)
            {
                datarow[i] = readers[i].readData();
            }

            return datarow;
        }
        catch (EOFException ex)
        {
            return null;
        }
    }

    @Override
    public void reset() throws IOException
    {
        for(DataReader reader: readers)
        {
            reader.reset();
        }
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return fieldMapping;
    }

    @Override
    public void close() throws IOException, ClassNotFoundException
    {
        for(DataReader reader: readers)
        {
            reader.close();
        }

    }


}
