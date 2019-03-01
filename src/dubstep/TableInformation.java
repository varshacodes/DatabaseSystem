package dubstep;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import java.util.List;
import java.util.HashMap;

public class TableInformation
{

    static HashMap<String, CreateTable> TableInfo;
    static HashMap<String, HashMap<String,Integer>> TableFieldPositionMapping;
    static HashMap<String, HashMap<String,FieldType>> TableFieldTypeMapping;
    static  HashMap<String, HashMap<Integer,String>> TablePositionFieldMapping;

    public static void addTableInfo(String TableName, CreateTable statement)
    {
        if(TableInfo == null)
        {
            TableInfo = new HashMap<String, CreateTable>();
        }

        TableInfo.put(TableName,statement);
        addFieldPositionMappingInfo(TableName,statement);
        addFieldTypeMappingInfo(TableName,statement);

    }
    public static void addFieldPositionMappingInfo(String TableName, CreateTable statement)
    {
        if(TableFieldPositionMapping == null && TablePositionFieldMapping == null)
        {
            TableFieldPositionMapping = new HashMap<String,HashMap<String, Integer>>();
            TablePositionFieldMapping = new HashMap<String,HashMap<Integer, String>>();

        }

        HashMap<String,Integer> FieldMapping = new HashMap<String,Integer>();
        HashMap<Integer,String> PositionMapping = new HashMap<Integer, String>();

        List<ColumnDefinition> columnDefinitionList = statement.getColumnDefinitions();

        for(int i =0; i < columnDefinitionList.size(); i++)
        {
            String FieldName = columnDefinitionList.get(i).getColumnName();
            FieldMapping.put(FieldName,i);
            PositionMapping.put(i,FieldName);
        }

        TableFieldPositionMapping.put(TableName,FieldMapping);
        TablePositionFieldMapping.put(TableName,PositionMapping);


    }
    public static void addFieldTypeMappingInfo(String TableName, CreateTable statement)
    {
        if(TableFieldTypeMapping == null)
        {
            TableFieldTypeMapping = new HashMap<String, HashMap<String, FieldType>>();
        }

        HashMap<String,FieldType> FieldMapping = new HashMap<String,FieldType>();

        List<ColumnDefinition> columnDefinitionList = statement.getColumnDefinitions();

        for(int i =0; i < columnDefinitionList.size(); i++)
        {
            String FieldName = columnDefinitionList.get(i).getColumnName();
            String FType = columnDefinitionList.get(i).getColDataType().toString();
            FieldMapping.put(FieldName,FieldType.getFieldType(FType));
        }

        TableFieldTypeMapping.put(TableName,FieldMapping);

    }


    public static  HashMap<Integer,String> getPositionFieldMapping(String TableName)
    {
        return  TablePositionFieldMapping.get(TableName);

    }

    public static HashMap<String,Integer> getFieldPostionMapping(String TableName)
    {
        return  TableFieldPositionMapping.get(TableName);

    }

    public static HashMap<String,FieldType> getFieldTypeMapping(String TableName)
    {
        return TableFieldTypeMapping.get(TableName);
    }



    public static HashMap<String,Integer> getFieldMappingwithAlias(String TableName,String Alias)
    {
        HashMap<Integer,String> Fieldmapping = TablePositionFieldMapping.get(TableName);

        HashMap<String,Integer> FieldMappingWithAlias = new HashMap<String,Integer>();

        for(int i=0; i < Fieldmapping.size(); i++)
        {
            String FieldName = Fieldmapping.get(i);
            FieldMappingWithAlias.put((Alias + "." + FieldName), i);
        }

        return FieldMappingWithAlias;
    }


    public static boolean hasTable(String TableName)
    {
        if(TableInfo.get(TableName)!=null )
        {
            return true;
        }
        else {

            return false;
        }
    }




}
