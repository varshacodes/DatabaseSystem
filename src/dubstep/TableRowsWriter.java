package dubstep;

import java.io.IOException;
import java.sql.SQLException;

public class TableRowsWriter
{
    String fileName;
    FieldType[] fieldTypes;
    String FolderName;
    DataWriter[] dataWriters;

    public TableRowsWriter(Table table) throws IOException
    {
        this.FolderName = table.getFolderName();
        this.fieldTypes =  table.getFieldTypes();
        setDataWriters();
    }

    private void setDataWriters() throws IOException
    {
        dataWriters = new DataWriter[fieldTypes.length];
        for(int i=0; i < fieldTypes.length; i++)
        {
            String fileName = this.FolderName+ "Field-"+(i+1);
            switch (fieldTypes[i])
            {
                case DOUBLE: dataWriters[i] = new DoubleWriter(fileName); break;
                case INT: dataWriters[i] = new LongWriter(fileName); break;
                case DATE: dataWriters[i] = new StringWriter(fileName); break;
                case STRING: dataWriters[i] = new StringWriter(fileName); break;
            }
        }
    }

    public Long writeTable(Table table) throws SQLException,IOException,ClassNotFoundException
    {
        RowIterator rowTraverser = new RowIterator(table.getTableName(),table.getFieldPostionMapping(),table.getFieldTypes());
        Long rows = writeTuples(rowTraverser);
        return rows;
    }

    private Long writeTuples(RowIterator rowTraverser) throws IOException
    {
        String Line = rowTraverser !=null ? rowTraverser.nextLine(): null;

        Long rows = Line !=null ? new Long(0): new Long(-1);

        while (Line != null)
        {
            rows = rows + 1;
            writeTuple(Line);
            Line = rowTraverser.nextLine();
        }

        return rows;
    }

    private void writeTuple(String Line) throws IOException
    {
        String[] datarow = Line.split("\\|");

        for(int i=0; i < dataWriters.length; i++)
        {
            dataWriters[i].writeData(datarow[i]);
        }
    }

    public void close()throws IOException
    {
        for(DataWriter writer: dataWriters)
        {
            writer.close();
        }
    }
}
