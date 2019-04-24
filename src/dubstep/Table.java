package dubstep;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Table
{
    String TableName;
    List<Field> fields;
    private static final String directory = "LoneWolf-Tables/";

    public Table(CreateTable statement)
    {
        this.TableName = statement.getTable().getWholeTableName();
        List<ColumnDefinition> columnDefinitionList = statement.getColumnDefinitions();
        this.fields = new ArrayList<>();

        for(int i =0; i < columnDefinitionList.size(); i++)
        {
            ColumnDefinition columnDef =  columnDefinitionList.get(i);
            String FieldName = columnDef.getColumnName();
            FieldType fieldType = FieldType.getFieldType(columnDef.getColDataType().toString());
            fields.add(new Field(FieldName,i,fieldType));
        }
    }

    public static String getDirectory()
    {
        return directory;
    }


    public String getFileName()
    {
        File dir = new File(directory);
        if(!dir.exists())
        {
            dir.mkdir();
        }
        return directory+TableName+ ".dat";

    }

    public String getTableName()
    {
        return TableName;
    }


    public List<Field> getFields()
    {
        return fields;
    }
}
