package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class Table
{
    String TableName;
    FieldType[] fieldTypes;
    String[] fieldNames;
    Long rows;
    UpdatesInfo updatesInfo;

    private static final String directory = "LoneWolf-";
    private static final String infoFileName = "Tables-Info.dat";

    public Table(CreateTable statement)
    {
        this.TableName = statement.getTable().getWholeTableName();
        rows = new Long(0);
        setColumnInfo(statement.getColumnDefinitions());
        updatesInfo = null;

    }

    public void insertRow(HashMap<String, PrimitiveValue> dataValues)
    {
        if(!hasUpdates())
        {
            updatesInfo = new UpdatesInfo();
        }
        PrimitiveValue[] dataRow = getDataRow(dataValues);
        updatesInfo.insertRow(dataRow);

    }
    public void insertRow(PrimitiveValue[] dataRow)
    {
        if(!hasUpdates())
        {
            updatesInfo = new UpdatesInfo();
        }
        updatesInfo.insertRow(dataRow);

    }

    public PrimitiveValue[] getDataRow(HashMap<String, PrimitiveValue> dataValues)
    {
        PrimitiveValue[] dataRow = new PrimitiveValue[fieldTypes.length];

        for(int i=0; i < fieldTypes.length; i++)
        {
            dataRow[i] = dataValues.getOrDefault(fieldNames[i],null);
        }
        return dataRow;
    }


    private void setColumnInfo(List<ColumnDefinition> columnDefinitionList)
    {
        this.fieldTypes = new FieldType[columnDefinitionList.size()];
        this.fieldNames = new String[columnDefinitionList.size()];
        for(int i =0; i < columnDefinitionList.size(); i++)
        {
            ColumnDefinition columnDef =  columnDefinitionList.get(i);
            fieldNames[i] = columnDef.getColumnName();
            fieldTypes[i] = FieldType.getFieldType(columnDef.getColDataType().toString());
        }

    }

    public String[] getFieldNames()
    {
        return fieldNames;
    }

    public Table(String tableName, FieldType[] fieldTypes, String[] fieldNames,Long rows)
    {
        TableName = tableName;
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
        this.rows = rows;
        updatesInfo = null;
    }

    public FieldType[] getFieldTypes()
    {
        return this.fieldTypes;
    }

    public static String getDirectory()
    {
        return directory;
    }


    public String getFolderName()
    {
        String folder = directory+this.TableName +"/";
        File dir = new File(folder);
        if(!dir.exists())
        {
            dir.mkdir();
        }
        return folder;

    }

    public String getTableName()
    {
        return TableName;
    }


    @Override
    public String toString()
    {
        return TableName +"  NoOfRows::"+ rows;
    }

    public HashMap<String,Integer> getFieldPostionMapping()
    {
        HashMap<String,Integer> fieldMapping = new HashMap<String, Integer>();

        for(int i=0; i < this.fieldNames.length; i++)
        {
            fieldMapping.put(TableName+"."+fieldNames[i],i);
            fieldMapping.put(fieldNames[i],i);
        }
        return fieldMapping;
    }

    public HashMap<String,Integer> getFieldMappingwithAlias(String Alias)
    {

        HashMap<String,Integer> FieldMappingWithAlias = new HashMap<String,Integer>();

        for(int i=0; i < this.fieldNames.length; i++)
        {
            FieldMappingWithAlias.put((Alias + "." + fieldNames[i]), i);
        }

        return FieldMappingWithAlias;
    }

    public HashMap<String,FieldType> getFieldTypeMapping()
    {
        HashMap<String,FieldType> FieldTypeMapping = new HashMap<String,FieldType>();

        for(int i=0; i < this.fieldNames.length; i++)
        {
            FieldTypeMapping.put(fieldNames[i], fieldTypes[i]);
            FieldTypeMapping.put(TableName+"."+fieldNames[i],fieldTypes[i]);

        }

        return FieldTypeMapping;

    }

    public static String getInfoFileName()
    {
        return infoFileName;
    }

    public void setRows(Long rows)
    {
        this.rows = rows;
    }

    public  boolean hasUpdates()
    {
        if(this.updatesInfo == null)
        {
            return  false;
        }
        return  true;
    }
    public UpdatesInfo getUpdatesInfo()
    {
        return updatesInfo;
    }

    public Long getRows()
    {
        return rows;
    }

    public RowTraverser getUpdatedIterator(RowTraverser rowTraverser,boolean isAllColumns)throws IOException
    {
        List<Integer> Fields = !isAllColumns && rowTraverser instanceof SelectTraverser? ((SelectTraverser) rowTraverser).getFields(): null;

        if(updatesInfo.isHasDeletes())
        {
            rowTraverser = new FilterIterator(rowTraverser,updatesInfo.getDeleteExpression());
        }

        if(updatesInfo.isHasInserts())
        {
            List<PrimitiveValue[]> projectedRows = null;
            if(isAllColumns)
            {
                projectedRows = updatesInfo.getDataRows();
            }
            else
            {
                projectedRows =  getProjectedRows(updatesInfo.getDataRows(),Fields);
            }
           InMemoryIterator inMemoryIterator = new InMemoryIterator(projectedRows,rowTraverser.getFieldPositionMapping(),rowTraverser.getNoOfFields());
           List<RowTraverser> rowTraversers = new ArrayList<>();
           rowTraversers.add(rowTraverser);
           rowTraversers.add(inMemoryIterator);
           rowTraverser = new UnionIterator(rowTraversers);
        }

        return rowTraverser;
    }

    public  List<PrimitiveValue[]> getProjectedRows(List<PrimitiveValue[]> dataRows, List<Integer> Fields)
    {

            List<PrimitiveValue[]> ProjectedRows = new ArrayList<>(dataRows.size());

            for(int i=0; i< dataRows.size(); i++)
            {
                ProjectedRows.add(getProjectedRow(dataRows.get(i), Fields));
            }

            return ProjectedRows;




    }

    public PrimitiveValue[] getProjectedRow(PrimitiveValue[] dataRow,List<Integer> Fields)
    {
        PrimitiveValue[] projectedRow = new PrimitiveValue[Fields.size()];
        Iterator<Integer> projectedFields = Fields.iterator();
        HashMap<String, Integer> fieldMapping = getFieldPostionMapping();
        int i=0;
        while (projectedFields.hasNext())
        {
            int position = projectedFields.next();;
            projectedRow[i++] = dataRow[position];
        }

        return projectedRow;

    }

    public void deleteRows(Expression whereCondition) throws SQLException
    {
        if(!hasUpdates())
        {
            updatesInfo = new UpdatesInfo();
        }

        updatesInfo.deleteRows(whereCondition);
    }

    public boolean hasUpdateColumns()
    {
        if(hasUpdates() && updatesInfo.isHasColumns())
        {
            return  true;
        }

        return  false;
    }

    public void addUpdateColumns(Set<String> columns)
    {
        Iterator<String> updateColumns = updatesInfo.getColumns().iterator();

        while (updateColumns.hasNext())
        {
            columns.add(updateColumns.next());
        }

    }
}
