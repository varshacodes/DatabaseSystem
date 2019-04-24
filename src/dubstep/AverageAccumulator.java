package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;

import java.sql.SQLException;
import java.util.HashMap;

public class AverageAccumulator implements Accumulator
{
    PrimitiveValue sum, count;
    ExpressionEvaluator evaluator;
    Expression expression;
    boolean isInitialized;


    @Override
    public String toString()
    {
        return "Sum: "+ sum + " Count:" + count;
    }

    public  AverageAccumulator(Expression expression)
    {
        count = new LongValue(0);
        this.expression = expression;
        this.isInitialized = false;
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow, HashMap<String, Integer> fieldMapping) throws SQLException
    {

        evaluator = new ExpressionEvaluator(dataRow,fieldMapping);
        if(!isInitialized)
        {
            sum = evaluator.eval(expression);
            count = evaluator.eval(new Addition(count,new LongValue(1)));
            isInitialized = true;
        }
        else
        {
            sum = evaluator.eval(new Addition(sum,evaluator.eval(expression)));
            count = evaluator.eval(new Addition(count,new LongValue(1)));

        }

    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {

        return evaluator.eval(new Division(sum,count));
    }
}
