package dubstep;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;

public class TableInformation
{

    static HashMap<String, CreateTable> TableInfo;
    static HashMap<String,List<Field>> TableFieldMappingInfo;

    public static void addTableInfo(Table table)
    {
        if(TableFieldMappingInfo == null)
        {
            TableFieldMappingInfo = new HashMap<String,List<Field>>();

        }
        TableFieldMappingInfo.put(table.getTableName(),table.getFields());

    }


    public static List<Field> getTableFieldMappingInfo(String TableName)
    {
        return TableFieldMappingInfo.get(TableName);
    }

    public  static FieldType[] getFieldTypes(String TableName)
    {
        List<Field> fieldList = TableFieldMappingInfo.get(TableName);
        FieldType []fieldTypes = new FieldType[fieldList.size()];

        for(int i=0; i < fieldList.size(); i++)
        {
            fieldTypes[i] = fieldList.get(i).fieldType;
        }
        return fieldTypes;
    }


    public static HashMap<String,Integer> getFieldPostionMapping(String TableName)
    {
        HashMap<String,Integer> joinMap = new HashMap<String, Integer>();

        List<Field> fieldList = TableFieldMappingInfo.get(TableName);

        for(int i=0; i < fieldList.size(); i++)
        {
            String field = fieldList.get(i).getColumnName();
            joinMap.put(TableName+"."+field,i);
            joinMap.put(field,i);
        }
        return joinMap;
    }


    public static HashMap<String,Integer> getFieldMappingwithAlias(String TableName,String Alias)
    {
        List<Field> Fieldmapping = TableFieldMappingInfo.get(TableName);

        HashMap<String,Integer> FieldMappingWithAlias = new HashMap<String,Integer>();

        for(int i=0; i < Fieldmapping.size(); i++)
        {
            String FieldName = Fieldmapping.get(i).getColumnName();
            FieldMappingWithAlias.put((Alias + "." + FieldName), i);
        }

        return FieldMappingWithAlias;
    }


    public static boolean hasTable(String TableName)
    {
        if(TableFieldMappingInfo.containsKey(TableName))
        {
            return true;
        }
        else {

            return false;
        }
    }


}
