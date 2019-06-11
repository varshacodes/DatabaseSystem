package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SelectTraverser implements RowTraverser
{

    String TableName;
    String FolderName;
    HashMap<String, Integer> FieldMapping;
    FieldType[] fieldTypes;
    DataReader[] readers;
    List<Integer> Fields;
    String[] FieldNames;
    boolean hasStar;



    public SelectTraverser(String TableName,Set selectedColumns,boolean hasTableAlias, String TableAlias)throws IOException
    {
        this.TableName = TableName;
        Table Table = TableInformation.getTable(TableName);
        this.FolderName = Table.getFolderName();
        this.fieldTypes =Table.getFieldTypes();
        this.FieldNames = Table.getFieldNames();
        this.hasStar = selectedColumns.contains("*");
        HashMap<String,Integer> fieldMapping = hasTableAlias ? Table.getFieldMappingwithAlias(TableAlias) : Table.getFieldPostionMapping();
        setFields(selectedColumns,fieldMapping);
        setDataReaders();
        setFieldMapping(Table,hasTableAlias,TableAlias);
    }

    public List<Integer> getFields()
    {
        return Fields;
    }

    private void setFieldMapping(Table table, boolean hasTableAlias, String TableAlias)
    {
        this.FieldMapping = new HashMap<>();

        for (int i=0; i < Fields.size(); i++)
        {
            int FieldPosition = Fields.get(i);
            String fieldName = FieldNames[FieldPosition];
            if(hasTableAlias)
            {
                FieldMapping.put(TableAlias+"."+fieldName,i);
            }
            else
            {
                FieldMapping.put(TableName+"."+fieldName,i);
                FieldMapping.put(fieldName,i);
            }
        }


    }

    private void setFields(Set selectedColumns, HashMap<String, Integer> fieldMapping)
    {
        Set<Integer>Fields = new HashSet<>();
        this.FieldMapping = new HashMap<>();
        Iterator<String> selectedFields = selectedColumns.iterator();
        int i=0;
        while (selectedFields.hasNext())
        {
            String FieldName = selectedFields.next();
            int position = fieldMapping.getOrDefault(FieldName,-1);
            if(position!=-1)
            {
                Fields.add(position);
                i++;
            }
        }
        if(!Fields.isEmpty())
        {
            this.Fields = new ArrayList<>();
            Iterator<Integer> fieldsIterator = Fields.iterator();
            while (fieldsIterator.hasNext())
            {
                this.Fields.add(fieldsIterator.next());
            }

        }
        else if(hasStar)
        {
            this.Fields = new ArrayList<>();
            this.Fields.add(0);
        }

    }
    private void setDataReaders()throws IOException
    {
        readers = new DataReader[Fields.size()];
        for(int i = 0; i < Fields.size(); i++)
        {
            int FieldPosition = Fields.get(i);
            String fileName = this.FolderName + "Field-" + (FieldPosition + 1);

            switch (this.fieldTypes[FieldPosition])
            {
                case DOUBLE:
                    readers[i] = new DoubleReader(fileName);
                    break;
                case INT:
                    readers[i] = new LongReader(fileName);
                    break;
                case DATE:
                    readers[i] = new DateReader(fileName);
                    break;
                case STRING:
                    readers[i] = new StringReader(fileName);
                    break;
            }
    }
    }

    @Override
    public PrimitiveValue[] next() throws IOException
    {
        try
        {
            PrimitiveValue[] datarow = new PrimitiveValue[readers.length];

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
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return FieldMapping;
    }

    @Override
    public int getNoOfFields()
    {
        return Fields.size();
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
    public void close() throws IOException, ClassNotFoundException
    {
        for(DataReader reader: readers)
        {
            reader.close();
        }

    }


}
