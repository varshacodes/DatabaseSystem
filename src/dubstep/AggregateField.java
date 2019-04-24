package dubstep;
import net.sf.jsqlparser.expression.Expression;

public class AggregateField
{
    Aggregate aggregate;
    Expression expression;
    boolean isAllColums;

    public AggregateField(Aggregate aggregate, Expression expression)
    {
        this.aggregate = aggregate;
        this.isAllColums = isAllColums;

        if(expression !=null)
        {
            this.expression = expression;
            isAllColums = false;
        }
        else
        {
            isAllColums = true;
        }

    }
    public Aggregate getAggregate()
    {
        return aggregate;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public boolean isAllColums()
    {
        return isAllColums;
    }


}
