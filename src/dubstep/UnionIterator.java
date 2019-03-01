package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UnionIterator implements RowTraverser
{

    List<RowTraverser> rowIteratorsList;
    RowTraverser currenIterator;
    int IteratorPosition;

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return currenIterator.getFieldPositionMapping();
    }

    @Override
    public void reset() throws IOException
    {
        IteratorPosition = 0;
       for(RowTraverser rowIterator: rowIteratorsList)
       {
           rowIterator.reset();
       }
    }

    public UnionIterator(List<RowTraverser> rowIteratorsList)
    {
        this.rowIteratorsList = rowIteratorsList;
        currenIterator = rowIteratorsList.get(0);
        IteratorPosition = 0;

    }


    @Override
    public PrimitiveValue[] next() throws SQLException, IOException
    {

        if(currenIterator != null)
        {
            PrimitiveValue[] dataRow = currenIterator.next();

            if((dataRow == null)&&(IteratorPosition < rowIteratorsList.size()-1))
            {
                currenIterator = rowIteratorsList.get(++IteratorPosition);
                return next();
            }

            return dataRow;
        }
        else {

            return null;
        }

    }

    @Override
    public boolean hasNext() throws IOException
    {
        if(currenIterator.hasNext())
        {
            return true;
        }
        else if(IteratorPosition < rowIteratorsList.size())
        {
            return true;
        }

        return false;
    }
}
