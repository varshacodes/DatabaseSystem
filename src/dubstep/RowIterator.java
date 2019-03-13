package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;

public class RowIterator implements RowTraverser
{
    String TableName;
    BufferedReader reader;
    HashMap<String,Integer> FieldPositionMapping;
    String csvFile;

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    public RowIterator(String TableName, HashMap<String,Integer> FieldPositionMapping) throws FileNotFoundException
    {
        ///Users/varshaganesh/IdeaProjects/team16/src/
        this.TableName = TableName;
        this.csvFile = "data/" + this.TableName + ".csv";
        this.reader = new BufferedReader(new FileReader(csvFile));
        this.FieldPositionMapping = FieldPositionMapping;
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


    public PrimitiveValue[] next() throws IOException
    {
        if (reader != null)
        {
            String Line = reader.readLine();

            if (Line != null)
            {
                HashMap<Integer, String> PositionFieldMapping = TableInformation.getPositionFieldMapping(TableName);
                HashMap<String, FieldType> FieldTypeMapping = TableInformation.getFieldTypeMapping(TableName);
                String columns[] = Line.split("\\|");
                PrimitiveValue[] dataRow = new PrimitiveValue[columns.length];


                for (int i = 0; i < columns.length; i++)
                {
                    String FieldName = PositionFieldMapping.get(i);
                    FieldType fieldType = FieldTypeMapping.get(FieldName);
                    PrimitiveValue value = FieldType.getPrimitiveValue(columns[i], fieldType);
                    dataRow[i] = value;
                }

                return dataRow;
            }

        }

        return null;

    }

    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return FieldPositionMapping;
    }
}