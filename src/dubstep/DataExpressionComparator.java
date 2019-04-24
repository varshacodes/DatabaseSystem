package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class DataExpressionComparator  implements Comparator<PrimitiveValue[]>
{

    List<OrderByField> orderByFields;
    HashMap<String, Integer> fieldMapping;

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
            ExpressionEvaluator aEvaluator = new ExpressionEvaluator(RowA, fieldMapping);
            ExpressionEvaluator bEvaluator = new ExpressionEvaluator(RowB, fieldMapping);

            for (int i = 0; i < orderByFields.size(); i++) {
                OrderByField orderByField = orderByFields.get(i);

                PrimitiveValue a = aEvaluator.eval(orderByField.getExpression());
                PrimitiveValue b = bEvaluator.eval(orderByField.getExpression());

                int compare = orderByField.isAscending() ? compare(a, b, aEvaluator) : -(compare(a, b, aEvaluator));
                if(compare !=0) return compare;

            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return 0;

    }

    private int compare(PrimitiveValue a, PrimitiveValue b, ExpressionEvaluator evaluator) throws SQLException
    {
        if(a.getType() == PrimitiveType.STRING)
        {
            return a.toRawString().compareTo(b.toRawString());
        }
        if(evaluator.eval(new EqualsTo(a,b)).toBool())
        {
            return 0;
        }
        else if(evaluator.eval(new GreaterThan(a,b)).toBool())
        {
            return 1;
        }
        else {
            return -1;
        }
    }

}
