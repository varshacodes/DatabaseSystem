package dubstep;

import net.sf.jsqlparser.expression.Expression;

public class OrderByField
{
    Expression expression;
    boolean isAscending;

    public OrderByField(Expression expression, boolean isAscending)
    {
        this.expression = expression;
        this.isAscending = isAscending;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public boolean isAscending()
    {
        return isAscending;
    }
}
