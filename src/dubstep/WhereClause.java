package dubstep;
import net.sf.jsqlparser.expression.Expression;

public class WhereClause
{
    Expression whereCondition;
    Field leftField;
    Field rightField;

    public WhereClause(Expression whereCondition, Field leftField, Field rightField)
    {
        this.whereCondition = whereCondition;
        this.leftField = leftField;
        this.rightField = rightField;
    }

    @Override
    public String toString()
    {
        return "WhereClause{" +
                "whereCondition=" + whereCondition +
                ", leftField=" + leftField +
                ", rightField=" + rightField +
                '}';
    }

    public Expression getWhereCondition()
    {
        return whereCondition;
    }


    public WhereClause(Expression whereCondition)
    {
        this.whereCondition = whereCondition;
    }

    public Field getLeftField()
    {
        return leftField;
    }

    public Field getRightField()
    {
        return rightField;
    }
}
