package dubstep;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class TableInformation
{

    static HashMap<String, CreateTable> TableInfo;
    static HashMap<String,Integer> TableMappingInfo;
    static List<Table> Tables;

    public static void addTableInfo(Table table)
    {
        if(TableMappingInfo == null)
        {
            TableMappingInfo = new HashMap<String,Integer>();
            Tables = new ArrayList<Table>();

        }
        TableMappingInfo.put(table.getTableName(),Tables.size());
        Tables.add(table);
    }

    public static Table getTable(String TableName)
    {
        int tableNo = TableMappingInfo.get(TableName);
        Table table = Tables.get(tableNo);
        return table;
    }

    public static  boolean hasUpdates(String TableName)
    {
        int tableNo = TableMappingInfo.get(TableName);
        Table table = Tables.get(tableNo);
        return table.hasUpdates();
    }


}
