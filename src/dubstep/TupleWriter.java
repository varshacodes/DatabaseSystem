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

    public TupleWriter(Table table) throws IOException
    {
        this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(table.getFileName())));
        this.fileName = table.getFileName();
        this.fieldTypes = TableInformation.getFieldTypes(table.getTableName());

    }

    private void writeTuples(RowTraverser rowTraverser)throws SQLException,IOException,ClassNotFoundException
    {
        PrimitiveValue[] dataRow = rowTraverser !=null ? rowTraverser.next(): null;

        while (dataRow != null)
        {
            writeTuple(dataRow);
            dataRow = rowTraverser.next();
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

    public void writeTable(Table table) throws SQLException,IOException,ClassNotFoundException
    {
        String csvFileName = "data/" + table.getTableName() + ".csv";
        RowTraverser rowTraverser = new RowIterator(csvFileName,TableInformation.getFieldPostionMapping(table.getTableName()),fieldTypes);
        writeTuples(rowTraverser);
    }
}
