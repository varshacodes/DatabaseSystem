package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.schema.Column;
import java.sql.SQLException;
import java.util.HashMap;


public class AverageAccumulator extends Evaluate implements Accumulator
{
    Addition sum;
    PrimitiveValue count;
    Expression expression;
    HashMap<String, Integer> fieldMapping;
    PrimitiveValue[] current;

    @Override
    public String toString()
    {
        return "Sum: "+ sum + " Count:" + count;
    }

    public  AverageAccumulator(Expression expression,HashMap<String, Integer> fieldMapping,PrimitiveValue[] dataRow)throws SQLException
    {
        this.expression = expression;
        this.fieldMapping = fieldMapping;
        init(dataRow);
    }

    public  void init(PrimitiveValue[] dataRow) throws SQLException
    {
        this.current = dataRow;
        sum = new Addition(expression,new LongValue(0));
        count = new LongValue(1);

    }


    @Override
    public void Accumulate(PrimitiveValue[] dataRow) throws SQLException
    {
        this.current = dataRow;
        sum.setLeftExpression(eval(sum));
        sum.setRightExpression(eval(expression));
        count = eval(new Addition(count, new LongValue(1)));

    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {
        return eval(new Division(sum,count));
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
