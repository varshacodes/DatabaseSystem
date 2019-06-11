package dubstep;

import java.io.*;

public class TableReader
{

    DataInputStream reader;
    String FileName;


    public TableReader() throws IOException
    {
        this.FileName = Table.getDirectory()+"Tables/"+ Table.getInfoFileName();
        this.reader = new DataInputStream(new BufferedInputStream(new FileInputStream(FileName)));
    }

    public Table readTable() throws IOException
    {
        String TableName = readTableName();
        int fieldsCount = readFieldsLength();
        FieldType fieldType[] = readFieldTypes(fieldsCount);
        String  FieldNames[] = readFieldNames(fieldsCount);
        Long rows = readNoOfRows();
        return new Table(TableName,fieldType,FieldNames,rows);

    }

    private Long readNoOfRows() throws IOException
    {
        return reader.readLong();
    }

    public void LoadTableInfo() throws IOException
    {
        try
        {
            Table table = reader != null ? readTable() : null;
            while (table != null)
            {
                TableInformation.addTableInfo(table);
                table = readTable();
            }
        }
        catch (EOFException e)
        {

        }
    }

     private  FieldType[] readFieldTypes(int fieldsCount)throws IOException
     {
         FieldType fieldTypes[] = new FieldType[fieldsCount];
         for(int i=0; i < fieldsCount; i++)
         {
             fieldTypes[i] = FieldType.getFieldType(reader.readInt());

         }
         return fieldTypes;
     }

     private String[] readFieldNames(int fieldsCount)throws IOException
     {
         String FieldNames[] = new String[fieldsCount];
         for(int i=0; i < fieldsCount; i++)
         {
             FieldNames[i] = reader.readUTF();
         }
         return FieldNames;
     }

    private int readFieldsLength() throws IOException
    {
        return reader.readInt();
    }

    private String readTableName() throws IOException
    {
        return reader.readUTF();
    }

}
