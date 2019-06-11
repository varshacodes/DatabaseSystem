package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OrderByIterator implements RowTraverser {

    RowTraverser rowIterator;
    HashMap<String,Integer> fieldMapping;
    HashMap<String,Integer> rowIteratorMapping;
    PrimitiveValue[] current;
    RowTraverser sortedIterator;
    boolean isInMemory;
    List<OrderByField> orderBy;
    boolean isInitialized;

    public OrderByIterator(RowTraverser rowIterator, List<OrderByElement> orderByFields, boolean isInMemory)
    {
        this.rowIterator = rowIterator;
        this.rowIteratorMapping = rowIterator.getFieldPositionMapping();
        this.isInMemory = isInMemory;
        this.isInitialized = false;
        parseOrderBy(orderByFields);


    }

    @Override
    public int getNoOfFields()
    {
        return rowIterator.getNoOfFields();
    }

    public void setRowIterator(RowTraverser rowIterator)
    {
        this.rowIterator = rowIterator;
    }

    private void initialize() throws SQLException,IOException,ClassNotFoundException
    {
        orderBy();
    }

    private void parseOrderBy(List<OrderByElement> orderByFields)
    {
        orderBy =  new ArrayList<>();

        for(int i=0; i< orderByFields.size(); i++)
        {
            Expression expression = orderByFields.get(i).getExpression();
            boolean isAsc = orderByFields.get(i).isAsc();
            orderBy.add(new OrderByField(expression,isAsc));
        }
    }

    private void orderBy() throws IOException,ClassNotFoundException,SQLException
    {

        if(isInMemory)
        {
            sortedIterator = new SortInMemory(rowIterator,orderBy,true);
        }
        else
        {
            sortedIterator = new SortOnDisk(rowIterator,orderBy,"OrderBy",true);
        }

    }


    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        if(!isInitialized)
        {
            initialize();
            isInitialized = true;
        }
        current = sortedIterator != null ? sortedIterator.next() : null;

        if(current != null)
        {
            return current;
        }
        return null;
    }


    public RowTraverser getChild()
    {
        return rowIterator;
    }

    @Override
    public void reset() throws IOException, SQLException, ClassNotFoundException
    {
        sortedIterator.reset();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping() {
        return this.rowIteratorMapping;
    }

    @Override
    public void close() throws IOException, ClassNotFoundException {
        sortedIterator.close();
    }



}
