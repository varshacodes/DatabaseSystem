package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TupleWriter
{
    DataOutputStream writer;
    String fileName;
    FieldType[] fieldTypes;

    public TupleWriter(String fileName, FieldType[] fieldTypes) throws FileNotFoundException
    {
        this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
        this.fileName = fileName;
        this.fieldTypes = fieldTypes;
    }

    public TupleWriter(Table Table) throws FileNotFoundException
    {
        this.fileName = Table.getFolderName()+Table.getTableName() +".dat";
        this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
        this.fieldTypes = Table.getFieldTypes();
    }



    private Long writeTuples(RowIterator rowTraverser)throws SQLException,IOException
    {
        String dataRow = rowTraverser !=null ? rowTraverser.nextLine(): null;

        Long rows = dataRow !=null ? new Long(0): new Long(-1);

        while (dataRow != null)
        {
            rows = rows + 1;
            writeTuple(dataRow);
            dataRow = rowTraverser.nextLine();
        }

        return rows;
    }

    public void writeTuple(String Line) throws IOException
    {
        String[] datarow = Line.split("\\|");
        for(int i=0; i < fieldTypes.length; i++)
        {
            switch (fieldTypes[i])
            {
                case INT: writer.writeLong(new Long(datarow[i])); break;
                case DATE: writer.writeUTF(datarow[i]); break;
                case STRING: writer.writeUTF(datarow[i]); break;
                case DOUBLE: writer.writeDouble(new Double(datarow[i])); break;
            }
        }
    }
    public void writeTuple(PrimitiveValue[] datarow) throws PrimitiveValue.InvalidPrimitive,IOException
    {
        for(int i=0; i < fieldTypes.length; i++)
        {
            switch (fieldTypes[i])
            {
                case INT: writer.writeLong(datarow[i].toLong()); break;
                case DATE: writer.writeUTF(datarow[i].toRawString()); break;
                case STRING: writer.writeUTF(datarow[i].toRawString()); break;
                case DOUBLE: writer.writeDouble(datarow[i].toDouble()); break;
            }
        }
    }

    public void writeTuples(List<PrimitiveValue[]> Tuples) throws PrimitiveValue.InvalidPrimitive,IOException
    {

        for(PrimitiveValue[] tuple: Tuples)
        {
            writeTuple(tuple);
        }
    }

    public void close()throws IOException
    {
        writer.close();
    }


}
