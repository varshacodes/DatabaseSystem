package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FilterIterator extends Evaluate implements RowTraverser
{
    RowTraverser dataRowIterator;
    HashMap<String,Integer> FieldPositionMapping;
    Expression whereCondition;
    PrimitiveValue[] current;

    public FilterIterator(RowTraverser dataRowIterator, Expression whereCondition)
    {
        this.dataRowIterator = dataRowIterator;
        this.FieldPositionMapping = FieldPositionMapping;
        this.whereCondition = whereCondition;
        this.FieldPositionMapping = dataRowIterator.getFieldPositionMapping();
    }

    @Override
    public int getNoOfFields()
    {
        return dataRowIterator.getNoOfFields();
    }

    public void setRowIterator(RowTraverser dataRowIterator)
    {
        this.dataRowIterator = dataRowIterator;
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
        dataRowIterator.close();

    }


    public PrimitiveValue[] next() throws SQLException, IOException,ClassNotFoundException
    {
        boolean isConditionTrue = false;

        while (!isConditionTrue)
        {
            current = dataRowIterator!= null ?dataRowIterator.next():null;

            if (current != null)
            {
                if(eval(whereCondition).toBool())
                {
                    return current;
                }
            }
            else
            {
                return null;
            }
        }

        return null;
    }


    @Override
    public void reset() throws IOException,SQLException,ClassNotFoundException
    {
        dataRowIterator.reset();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return FieldPositionMapping;
    }

    public RowTraverser getChild() {
        return dataRowIterator;
    }

    public Expression getWhereCondition()
    {
        return whereCondition;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = this.FieldPositionMapping.get(column.toString());
        return current[position];
    }



}
