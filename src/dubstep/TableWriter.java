package dubstep;

import java.io.*;

public class TableWriter
{
    DataOutputStream writer;
    String fileName;

    public TableWriter()throws IOException
    {
        String directory = Table.getDirectory()+"Tables/";
        File dir = new File(directory);
        if(!dir.exists())
        {
            dir.mkdir();
        }
        this.fileName = directory+Table.getInfoFileName();
    }

    private void open()throws IOException
    {
        this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName,true)));

    }

    public void writeTable(Table table) throws IOException
    {
        open();
        writeTableName(table.getTableName());
        writeFieldsLength(table.getFieldTypes().length);
        writeFieldTypes(table.getFieldTypes());
        writeFieldNames(table.getFieldNames());
        writeNoOfRows(table.getRows());
        close();
    }

    private void writeNoOfRows(Long rows) throws IOException
    {
        writer.writeLong(rows);
    }


    private void writeTableName(String tableName) throws IOException
    {
        writer.writeUTF(tableName);
    }

    private void writeFieldsLength(int length) throws IOException
    {
        writer.writeInt(length);
    }

    private void writeFieldTypes(FieldType[] fieldTypes) throws IOException
    {
        for(int i=0; i < fieldTypes.length; i++)
        {
            writer.writeInt(fieldTypes[i].getFieldType());
        }
    }

    private void writeFieldNames(String[] fieldNames) throws IOException
    {
        for(int i=0; i < fieldNames.length; i++)
        {
            writer.writeUTF(fieldNames[i]);
        }
    }

    public void close() throws IOException
    {
        writer.close();
    }
}
