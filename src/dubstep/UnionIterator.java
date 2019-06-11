package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class UnionIterator implements RowTraverser
{

    List<RowTraverser> rowIteratorsList;
    RowTraverser currenIterator;
    int IteratorPosition;
    PrimitiveValue[] current;


    @Override
    public int getNoOfFields()
    {
        return currenIterator.getNoOfFields();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return currenIterator.getFieldPositionMapping();
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
        for(RowTraverser rowIterator: rowIteratorsList)
        {
            rowIterator.close();
        }

    }

    @Override
    public void reset() throws IOException,SQLException,ClassNotFoundException
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
    public PrimitiveValue[] next() throws SQLException, IOException,ClassNotFoundException
    {

        if(currenIterator != null)
        {
            PrimitiveValue[] dataRow = currenIterator.next();

            if((dataRow == null)&&(IteratorPosition < rowIteratorsList.size()-1))
            {
                currenIterator = rowIteratorsList.get(++IteratorPosition);
                return next();
            }

            current = dataRow;
            return dataRow;
        }
        else {

            current = null;
            return null;
        }

    }


}
