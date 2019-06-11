package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class InMemoryIterator implements RowTraverser
{
    List<PrimitiveValue[]> Rows;
    Iterator<PrimitiveValue[]> rowsIterator;
    HashMap<String,Integer> fieldPositionMapping;
    int NoOfFields;

    public  InMemoryIterator(List<PrimitiveValue[]> rows,HashMap<String,Integer> fieldPositionMapping, int NoOfFields)
    {
        this.Rows = rows;
        this.rowsIterator = rows.iterator();
        this.fieldPositionMapping = fieldPositionMapping;
        this.NoOfFields = NoOfFields;
    }

    @Override
    public int getNoOfFields()
    {
        return NoOfFields;
    }

    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        if(rowsIterator.hasNext())
        {
            return  rowsIterator.next();
        }

        return null;
    }

    @Override
    public void reset() throws IOException, SQLException, ClassNotFoundException
    {
        this.rowsIterator = this.Rows.iterator();

    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return fieldPositionMapping;
    }

    @Override
    public void close() throws IOException, ClassNotFoundException
    {
        Rows = null;

    }
}
