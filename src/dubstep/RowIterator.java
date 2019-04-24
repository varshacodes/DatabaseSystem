package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.io.File;

public class RowIterator implements RowTraverser
{
    String TableName;
    BufferedReader reader;
    HashMap<String,Integer> FieldPositionMapping;
    String csvFile;
    PrimitiveValue[] current;
    FieldType []FieldTypes;

    public RowIterator(String TableName) throws FileNotFoundException
    {
        this.TableName = TableName;
        this.csvFile = "data/" + this.TableName + ".csv";
        this.reader = new BufferedReader(new FileReader(csvFile));
        this.FieldTypes = TableInformation.getFieldTypes(TableName);
        this.FieldPositionMapping = TableInformation.getFieldPostionMapping(TableName);
    }

    public RowIterator(String TableName,  HashMap<String,Integer> FieldPositionMapping) throws FileNotFoundException
    {
        this.TableName = TableName;
        this.csvFile = "data/" + this.TableName + ".csv";
        this.reader = new BufferedReader(new FileReader(csvFile));
        this.FieldPositionMapping = FieldPositionMapping;
        this.FieldTypes = TableInformation.getFieldTypes(TableName);

    }

    public RowIterator(String FileName, HashMap<String,Integer> FieldPositionMapping, FieldType[] fieldTypes) throws FileNotFoundException
    {
        this.csvFile = FileName;
        this.reader = new BufferedReader(new FileReader(csvFile));
        this.FieldPositionMapping = FieldPositionMapping;
        this.FieldTypes= fieldTypes;
    }


    public void reset() throws IOException
    {
        reader = new BufferedReader(new FileReader(csvFile));
    }

    public boolean hasNext() throws IOException
    {
        reader.mark(100000);

        if (reader.readLine() != null)
        {
            reader.reset();
            return true;
        }

        return false;
    }


    public PrimitiveValue[] next() throws IOException, SQLException
    {
        if (reader != null)
        {
            String Line = reader.readLine();

            if (Line != null)
            {
                String columns[] = Line.split("\\|");
                PrimitiveValue[] dataRow = new PrimitiveValue[columns.length];
                for (int i = 0; i < columns.length; i++)
                {
                    PrimitiveValue value = FieldType.getPrimitiveValue(columns[i], this.FieldTypes[i]);
                    dataRow[i] = value;
                }

                return dataRow;
            }
        }

        return  null;
    }

    public PrimitiveValue[] getcurrent()
    {
        return current;
    }

    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return FieldPositionMapping;
    }


    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    public String getTableName()
    {
        return TableName;
    }

}