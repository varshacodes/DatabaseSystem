package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;

public class DataRowComparator implements Comparator<PrimitiveValue[]>, Serializable
{
    List<Field> sortFields;
    ExpressionEvaluator evaluator;
    boolean []isDescending;

    public DataRowComparator(List<Field> sortFields)
    {
        this.sortFields = sortFields;
    }

    @Override
    public int compare(PrimitiveValue[] rowA, PrimitiveValue[] rowB)
    {
        try {
            this.evaluator = new ExpressionEvaluator(rowA,null);
            for (Field sortField : sortFields)
            {
                int compare = compare(rowA[sortField.getPosition()], rowB[sortField.getPosition()]);
                if(compare!=0)
                {
                    return compare;
                }

            }
            return 0;
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
