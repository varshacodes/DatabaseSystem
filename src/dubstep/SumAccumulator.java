package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import java.sql.SQLException;
import java.util.HashMap;

public class SumAccumulator extends Evaluate implements  Accumulator
{
    Addition sum;
    Expression expression;
    HashMap<String, Integer> fieldMapping;
    PrimitiveValue[] current;
    boolean isInitialized;

    @Override
    public String toString()
    {
       return " sum: "+ sum;
    }

    public SumAccumulator(Expression expression, HashMap<String, Integer> fieldMapping,PrimitiveValue[] dataRow)throws SQLException
    {
       this.expression = expression;
       this.isInitialized = false;
       this.fieldMapping = fieldMapping;
       init(dataRow);
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow)throws SQLException
    {
        this.current = dataRow;
        sum.setLeftExpression(eval(sum));
        sum.setRightExpression(eval(expression));
    }

    @Override
    public PrimitiveValue Fold()throws SQLException
    {
        return eval(sum);
    }

    @Override
    public void init(PrimitiveValue[] dataRow) throws SQLException
    {
        this.current = dataRow;
        sum = new Addition(eval(expression),new LongValue(0));

    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
