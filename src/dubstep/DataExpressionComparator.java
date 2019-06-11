package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class DataExpressionComparator extends Evaluate implements Comparator<PrimitiveValue[]>
{

    List<OrderByField> orderByFields;
    HashMap<String, Integer> fieldMapping;
    PrimitiveValue[] current;

    public DataExpressionComparator(List<OrderByField> orderByFields, HashMap<String, Integer> fieldMapping)
    {
        this.orderByFields = orderByFields;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public int compare(PrimitiveValue[] RowA, PrimitiveValue[] RowB)
    {
        try
        {

            for (int i = 0; i < orderByFields.size(); i++)
            {
                OrderByField orderByField = orderByFields.get(i);
                current = RowA;
                PrimitiveValue a = eval(orderByField.getExpression());
                current = RowB;
                PrimitiveValue b = eval(orderByField.getExpression());

                int compare = orderByField.isAscending() ? compare(a, b) : -(compare(a, b));
                if(compare !=0) return compare;

            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return 0;

    }

    private int compare(PrimitiveValue a, PrimitiveValue b) throws SQLException
    {
        if(a.getType() == PrimitiveType.STRING)
        {
            return a.toRawString().compareTo(b.toRawString());
        }
        if(eval(new EqualsTo(a,b)).toBool())
        {
            return 0;
        }
        else if(eval(new GreaterThan(a,b)).toBool())
        {
            return 1;
        }
        else {
            return -1;
        }
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
