package dubstep;
import java.util.*;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import java.sql.SQLException;


public abstract class Evaluate extends Eval
{
    public PrimitiveValue eval(Expression e) throws SQLException
    {
        if(e instanceof BinaryExpression){ return eval((BinaryExpression)e); }
        else if(e instanceof Column){ return eval((Column)e); }
        else if(e instanceof PrimitiveValue){ return (PrimitiveValue) e; }
        else if(e instanceof InverseExpression){ return eval((InverseExpression)e); }
        else if(e instanceof CaseExpression){ return eval((CaseExpression)e); }
        else if(e instanceof WhenClause){ return eval((WhenClause)e); }
        else if(e instanceof AllComparisonExpression){ return eval((AllComparisonExpression)e); }
        else if(e instanceof AnyComparisonExpression){ return eval((AnyComparisonExpression)e); }
        else if(e instanceof Between){ return eval((Between)e); }
        else if(e instanceof ExistsExpression){ return eval((ExistsExpression)e); }
        else if(e instanceof InExpression){ return eval((InExpression)e); }
        else if(e instanceof Function){ return eval((Function)e); }
        else if(e instanceof IsNullExpression){ return eval((IsNullExpression)e); }
        else if(e instanceof JdbcParameter){ return eval((JdbcParameter)e); }
        throw new SQLException("Invalid operator: "+e);
    }


    public PrimitiveValue eval(BinaryExpression e)throws SQLException
    {
        if(e instanceof Addition){ return eval((Addition)e); }
        else if(e instanceof Division){ return eval((Division)e); }
        else if(e instanceof Multiplication){ return eval((Multiplication)e); }
        else if(e instanceof Subtraction){ return eval((Subtraction)e); }
        else if(e instanceof AndExpression){ return eval((AndExpression)e); }
        else if(e instanceof OrExpression){ return eval((OrExpression)e); }
        else if(e instanceof EqualsTo){ return eval((EqualsTo)e); }
        else if(e instanceof NotEqualsTo){ return eval((NotEqualsTo)e); }
        else if(e instanceof GreaterThan){ return eval((GreaterThan)e); }
        else if(e instanceof GreaterThanEquals){ return eval((GreaterThanEquals)e); }
        else if(e instanceof MinorThan){ return eval((MinorThan)e); }
        else if(e instanceof MinorThanEquals){ return eval((MinorThanEquals)e); }
        else if(e instanceof LikeExpression){ return eval((LikeExpression)e); }
        else if(e instanceof Matches){ return eval((Matches)e); }
        else if(e instanceof BitwiseXor){ return eval((BitwiseXor)e); }
        else if(e instanceof BitwiseOr){ return eval((BitwiseOr)e); }
        else if(e instanceof BitwiseAnd){ return eval((BitwiseAnd)e); }
        else if(e instanceof Concat){ return eval((Concat)e); }
        return null;
    }

    public boolean eval(List<Expression> whereClauses)throws SQLException
    {
        boolean isTrue = true;
        for(Expression expression: whereClauses)
        {
            isTrue = isTrue && eval(expression).toBool();

           if(!isTrue)
           {
               return  false;
           }
        }

        return true;

    }



    public PrimitiveValue eval(AndExpression a) throws SQLException
    {
        PrimitiveValue lhs = eval(a.getLeftExpression());
        if(lhs.toBool())
        {
            PrimitiveValue rhs = eval(a.getRightExpression());
            return  rhs;
        }
        else
        {
            return lhs;

        }
    }
    public PrimitiveValue eval(OrExpression a) throws SQLException
    {
        PrimitiveValue lhs = eval(a.getLeftExpression());

        if(!lhs.toBool())
        {
            PrimitiveValue rhs = eval(a.getRightExpression());
            return  rhs;
        }
        else
        {
            return lhs;

        }
    }



}
