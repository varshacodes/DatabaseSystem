package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

public class LimitIterator implements RowTraverser
{

    RowTraverser rowIterator;
    HashMap<String, Integer> fieldPositionMapping;
    long limit;
    long count;
    PrimitiveValue[] current;


    public LimitIterator(RowTraverser rowIterator,long limit)
    {
        this.rowIterator = rowIterator;
        this.limit = limit;
        this.fieldPositionMapping = rowIterator.getFieldPositionMapping();
        this.count = 1;

    }

    public void setRowIterator(RowTraverser rowIterator)
    {
        this.rowIterator = rowIterator;
    }

    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        current = rowIterator!=null ? rowIterator.next(): null;

        if(current !=null && count <= limit)
        {
            count = count + 1;
            return current;
        }

        return null;


    }

    @Override
    public void reset() throws IOException, SQLException, ClassNotFoundException
    {
        rowIterator.reset();
        count = 1;
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping() {
        return this.fieldPositionMapping;
    }

    @Override
    public void close() throws IOException, ClassNotFoundException {
        rowIterator.close();
    }

    @Override
    public PrimitiveValue[] getcurrent() {
        return new PrimitiveValue[0];
    }

    public RowTraverser getChild()
    {
        return rowIterator;
    }

}
