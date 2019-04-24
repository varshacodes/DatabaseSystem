package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import java.util.Arrays;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;


public class Optimizer extends Eval
{

    RowTraverser Traverser;
    boolean isInMemory;
    HashMap<ConditionType,Boolean> conditionMapping;
    List<Expression> Top;
    List<Expression> Left;
    List<Expression> Right;
    WhereClause Join;
    List<Column> columns;

    public Optimizer(RowTraverser Traverser, boolean isInMemory)throws IOException,SQLException,ClassNotFoundException
    {
        this.isInMemory = isInMemory;
        this.Traverser = Traverser;

    }

    public RowTraverser optimize()throws IOException,SQLException,ClassNotFoundException
    {
        if(Traverser instanceof RowIterator)
        {
            return Traverser;
        }
        else  if(Traverser instanceof TupleReader)
        {
            return Traverser;
        }
        else if (Traverser instanceof  ProjectIterator)
        {
            Optimizer optimizer = new Optimizer(((ProjectIterator) Traverser).getChild(),isInMemory);
            ((ProjectIterator) Traverser).setRowIterator(optimizer.optimize());
            return Traverser;
        }
        else if(Traverser instanceof AggregateIterator)
        {
            Optimizer optimizer = new Optimizer(((AggregateIterator) Traverser).getChild(),isInMemory);
            ((AggregateIterator) Traverser).setRowIterator(optimizer.optimize());
            return Traverser;
        }
        else if(Traverser instanceof OrderByIterator)
        {
            Optimizer optimizer = new Optimizer(((OrderByIterator) Traverser).getChild(),isInMemory);
            ((OrderByIterator) Traverser).setRowIterator(optimizer.optimize());
            return Traverser;

        }
        else if(Traverser instanceof LimitIterator)
        {
            Optimizer optimizer = new Optimizer(((LimitIterator) Traverser).getChild(),isInMemory);
            ((LimitIterator) Traverser).setRowIterator(optimizer.optimize());
            return Traverser;
        }
        else if(Traverser instanceof FilterIterator)
        {
            RowTraverser child = ((FilterIterator) Traverser).getChild();

            if(child instanceof JoinIterator)
            {
                ArrayList<Expression> whereClauses = splitWhereClauses(((FilterIterator) Traverser).getWhereCondition(),new ArrayList<Expression>());
                RowTraverser left = ((JoinIterator) child).getLeftChild();
                RowTraverser right = ((JoinIterator) child).getRightChild();
                Categorize(whereClauses,left.getFieldPositionMapping(),right.getFieldPositionMapping());

                if(conditionMapping.containsKey(ConditionType.LEFT))
                {
                    Optimizer optimizer = new Optimizer(new FilterIterator(left,getWhereCondition(Left)),isInMemory);
                    left = optimizer.optimize();
                    ((JoinIterator) child).setLeftIterator(left);
                }
                if(conditionMapping.containsKey(ConditionType.RIGHT))
                {
                    Optimizer optimizer = new Optimizer(new FilterIterator(right,getWhereCondition(Right)),isInMemory);
                    right = optimizer.optimize();
                    ((JoinIterator) child).setRightIterator(right);
                }
                if(conditionMapping.containsKey(ConditionType.JOIN))
                {
                    if(isInMemory)
                    {
                        child = new OnePassHashJoin(left,right,Join.getLeftField().getPosition(),Join.getRightField().getPosition());
                    }
                    else
                    {
                        List<Field> leftFields = Arrays.asList(Join.getLeftField());
                        List<Field> rightFields =Arrays.asList(Join.getRightField());
                        child = new SortMergeJoin(left,right,leftFields,rightFields);

                    }
                }
                if(conditionMapping.containsKey(ConditionType.TOP))
                {
                    Traverser = new FilterIterator(child,getWhereCondition(Top));
                }
                else
                {
                    Traverser = new ProjectIterator(child,null,true);
                }
                return Traverser;
            }
            else
            {
                Optimizer optimizer = new Optimizer(child,isInMemory);
                ((FilterIterator) Traverser).setRowIterator(optimizer.optimize());
                return Traverser;
            }
        }

        return Traverser;

    }


    private void Categorize(ArrayList<Expression> whereClauses, HashMap<String, Integer> leftMapping, HashMap<String, Integer> rightMapping)throws SQLException
    {
        conditionMapping = new HashMap<ConditionType,Boolean>();

        for(Expression expression : whereClauses)
        {
           parseWhereClause(expression,leftMapping,rightMapping);
        }
    }

    private void parseWhereClause(Expression whereClause,HashMap<String, Integer> leftMapping, HashMap<String, Integer> rightMapping)throws SQLException
    {

        if(whereClause instanceof EqualsTo)
        {
            parseEqualsToCondition(whereClause,leftMapping, rightMapping);
        }
        else
        {
            columns = getColumns(whereClause);
            categorizeCondition(whereClause,columns,leftMapping,rightMapping);
        }
    }

    private void parseEqualsToCondition(Expression whereClause, HashMap<String, Integer> leftMapping, HashMap<String, Integer> rightMapping)throws SQLException
    {
        Expression first = ((EqualsTo) whereClause).getLeftExpression();
        Expression second = ((EqualsTo) whereClause).getRightExpression();

        if(first instanceof Column && second instanceof Column)
        {
            if(leftMapping.containsKey(first.toString()) && leftMapping.containsKey(second.toString()))
            {
                if(conditionMapping.containsKey(ConditionType.LEFT))
                {
                    Left.add(whereClause);
                }
                else
                {
                    Left = new ArrayList<Expression>();
                    Left.add(whereClause);
                    conditionMapping.put(ConditionType.LEFT,true);
                }
            }
            else if(rightMapping.containsKey(first.toString()) && rightMapping.containsKey(second.toString()))
            {
                if(conditionMapping.containsKey(ConditionType.RIGHT))
                {
                    Right.add(whereClause);
                }
                else
                {
                    Right = new ArrayList<Expression>();
                    Right.add(whereClause);
                    conditionMapping.put(ConditionType.RIGHT,true);
                }
            }
            else if(!conditionMapping.containsKey(ConditionType.JOIN))
            {
                 String leftName  = leftMapping.containsKey(first.toString())? first.toString() : second.toString();
                    int leftIndex = leftMapping.get(leftName);
                    String rightName = rightMapping.containsKey(first.toString())?first.toString() : second.toString();
                    int rightIndex = rightMapping.get(rightName);
                    Field Left = new Field(leftName,leftIndex,null);
                    Field right = new Field(rightName,rightIndex,null);
                    Join = new WhereClause(whereClause,Left,right);
                    conditionMapping.put(ConditionType.JOIN,true);
            }
            else {
                    if(conditionMapping.containsKey(ConditionType.TOP))
                    {
                        Top.add(whereClause);
                    }
                    else
                    {
                        Top = new ArrayList<Expression>();
                        Top.add(whereClause);
                        conditionMapping.put(ConditionType.TOP,true);
                    }
                }
        }
        else
        {
            columns = getColumns(whereClause);
            categorizeCondition(whereClause,columns,leftMapping,rightMapping);
        }
    }

    private void categorizeCondition(Expression whereClause, List<Column> columns, HashMap<String, Integer> leftMapping, HashMap<String, Integer> rightMapping)
    {
        boolean isAllLeft = isAllPresent(columns,leftMapping);
        boolean isAllRight = isAllPresent(columns,rightMapping);

        if(isAllLeft)
        {
            if(conditionMapping.containsKey(ConditionType.LEFT))
            {
                Left.add(whereClause);
            }
            else
            {
                Left = new ArrayList<Expression>();
                Left.add(whereClause);
                conditionMapping.put(ConditionType.LEFT,true);
            }
        }
        else if(isAllRight)
        {
            if(conditionMapping.containsKey(ConditionType.RIGHT))
            {
                Right.add(whereClause);
            }
            else
            {
                Right = new ArrayList<Expression>();
                Right.add(whereClause);
                conditionMapping.put(ConditionType.RIGHT,true);
            }
        }
        else
        {
            if(conditionMapping.containsKey(ConditionType.TOP))
            {
                Top.add(whereClause);
            }
            else
            {
                Top = new ArrayList<Expression>();
                Top.add(whereClause);
                conditionMapping.put(ConditionType.TOP,true);
            }

        }
    }

    private boolean isAllPresent(List<Column> columns, HashMap<String, Integer> leftMapping)
    {
        for(Column column: columns)
        {
            if(!leftMapping.containsKey(column.toString()))
            {
                return false;
            }
        }
        return true;
    }


    private ArrayList<Expression> splitWhereClauses(Expression whereCondition,ArrayList<Expression> whereClauses)
    {

        if(whereCondition instanceof AndExpression)
        {
           Expression left = ((AndExpression) whereCondition).getLeftExpression();

           if(left instanceof AndExpression)
           {
               splitWhereClauses(left,whereClauses);
           }
           else
           {
               whereClauses.add(left);
           }

           Expression right = ((AndExpression) whereCondition).getRightExpression();

           if(right instanceof AndExpression)
           {
               splitWhereClauses(right,whereClauses);

           }
           else
           {
               whereClauses.add(right);
           }
        }
        else
        {
            whereClauses.add(whereCondition);
        }

        return whereClauses;

    }

    public List<Column> getColumns(Expression expression) throws SQLException
    {
        columns = new ArrayList<>();
        eval(expression);
        return columns;

    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        columns.add(column);
        return null;
    }

    public Expression getWhereCondition(List<Expression> conditions)
    {
        if(conditions.size() == 1)
        {
           return conditions.get(0);
        }
        else
        {
            Expression expression = new AndExpression(conditions.get(0),conditions.get(1));

            for(int i = 2; i < conditions.size();i++)
            {
                expression = new AndExpression(expression,conditions.get(i));
            }

            return expression;
        }
    }



}
