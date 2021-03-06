package dubstep;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;


public class SelectProcessor extends Evaluate
{
    public static final int FROM_TABLE = 201;
    public static final int FROM_JOIN = 202;
    public static final int FROM_SUBSELECT = 203;
    public static final int PLAIN_SELECT= 301;
    public static final int UNION_SELECT = 302;
    boolean isAllcolumns;
    boolean hasWhereCondition;
    boolean isAggregate;
    boolean hasTableAlias;
    boolean inMemory;
    boolean hasOrderBy;
    boolean hasGroupBy;
    boolean hasHavingCondition;
    boolean hasLimitBy;
    boolean hasJoins;
    boolean hasQueryAlias;
    int selectType;
    int FromType;
    Expression havingCondition;
    Long limitValue;
    List<Column> groupByColumns;
    List<PlainSelect> PlainSelectList;
    List<OrderByElement> orderByFields;
    List<SelectItem> selectItems;
    RowTraverser JoinIterator;
    SelectBody selectBody;
    SelectBody subSelectBody;
    Expression whereCondition;
    String TableName;
    String LeftTableAlias;
    String queryAlias;
    Set<String> selectedColumns;
    boolean isOptimizable;

    public boolean isOptimizable()
    {
        return isOptimizable;
    }

    public boolean isHasJoins()
    {
        return hasJoins;
    }

    public SelectProcessor(SelectBody selectBody, boolean inMemory, String queryAlias) throws SQLException,ClassNotFoundException,IOException
    {
       this.selectBody = selectBody;
       this.inMemory = inMemory;
       this.hasQueryAlias = queryAlias!=null ? true : false;
       this.queryAlias = queryAlias;
       this.isOptimizable = false;
       parseSelectBody();
    }


    private void parseSelectBody() throws SQLException,ClassNotFoundException,IOException
    {
        if(selectBody instanceof PlainSelect)
        {
            selectType = PLAIN_SELECT;
            parseWhereCondition();
            parseOrderBy();
            parseGroupBy();
            parseSelectItem();
            parseFromItem();
            parseHavingCondition();
            parseLimitBy();
            if(this.hasWhereCondition && this.hasJoins)
            {
                this.isOptimizable = true;
            }

        }
        else if(selectBody instanceof Union)
        {
          selectType = UNION_SELECT;
          PlainSelectList = ((Union) selectBody).getPlainSelects();

        }
    }

    private void parseLimitBy()
    {

        Limit limit = ((PlainSelect)selectBody).getLimit();
        if(limit != null)
        {
            limitValue = limit.getRowCount();
            hasLimitBy = true;
        }


    }

    private void parseHavingCondition() {

        havingCondition = ((PlainSelect) selectBody).getHaving();
        if(havingCondition != null)
        {
            hasHavingCondition = true;
        }
        else
        {
            hasHavingCondition  = false;
        }


    }

    private void parseOrderBy()
    {
        orderByFields = ((PlainSelect)selectBody).getOrderByElements();
        if(orderByFields !=null)
        {
            hasOrderBy = true;
        }

    }

    private void parseFromItem()throws SQLException,ClassNotFoundException,IOException
    {
        FromItem fromItem = ((PlainSelect)selectBody).getFromItem();
        List<Join> joins= ((PlainSelect) selectBody).getJoins();
        this.hasJoins = joins !=null? true: false;

        if(hasJoins)
        {
            FromType = FROM_JOIN;
            RowTraverser left = parseFromItem(fromItem);
            RowTraverser right = parseFromItem(joins.get(0).getRightItem());
            this.JoinIterator = new JoinIterator(left,right);
            for(int i=1; i < joins.size(); i++)
            {
                fromItem = joins.get(i).getRightItem();
                right = parseFromItem(fromItem);
                this.JoinIterator = new JoinIterator(JoinIterator,right);
            }
        }
        else if(fromItem instanceof Table)
        {
            FromType = FROM_TABLE;
            TableName = ((Table)fromItem).getWholeTableName();
            if(fromItem.getAlias() != null)
            {
                hasTableAlias = true;
                LeftTableAlias = fromItem.getAlias();
            }
        }
        else if(fromItem instanceof SubSelect)
        {
            FromType = FROM_SUBSELECT;
            subSelectBody = ((SubSelect) fromItem).getSelectBody();
            queryAlias = fromItem.getAlias();
        }

    }

    public boolean isHasWhereCondition()
    {
        return hasWhereCondition;
    }

    private RowTraverser parseFromItem(FromItem fromItem) throws SQLException,ClassNotFoundException,IOException
    {
        RowTraverser rowTraverser = null;

        if(fromItem instanceof Table)
        {
            TableName = ((Table)fromItem).getWholeTableName();
            hasTableAlias = fromItem.getAlias() != null? true: false;
            LeftTableAlias = hasTableAlias ? fromItem.getAlias(): null;
            rowTraverser = getTableTraverser(TableName,isAllcolumns,hasTableAlias,LeftTableAlias,selectedColumns);

        }
        else if(fromItem instanceof SubSelect)
        {
            SelectBody subSelectBody = ((SubSelect) fromItem).getSelectBody();
            SelectProcessor subSelectProcessor = new SelectProcessor(subSelectBody,inMemory,fromItem.getAlias());
            rowTraverser = subSelectProcessor.processQuery();
            if(subSelectProcessor.isHasJoins()&&subSelectProcessor.isHasWhereCondition())
            {
                isOptimizable = true;
            }
        }

        return rowTraverser;
    }

    private void parseGroupBy()
    {
        groupByColumns = ((PlainSelect)selectBody).getGroupByColumnReferences();
        if(groupByColumns != null)
        {
           hasGroupBy = true;
           isAllcolumns = true;
        }
    }

    private void parseSelectItem()throws SQLException
    {
        selectItems = ((PlainSelect)selectBody).getSelectItems();
        SelectItem selectItem = selectItems.get(0);

        if((selectItem instanceof AllColumns)|| (selectItem instanceof  AllTableColumns))
        {
            isAllcolumns = true;

        }
        else {
                 Expression expression = ((SelectExpressionItem)selectItem).getExpression();
                 if(expression instanceof Function)
                 {
                     isAggregate = true;
                 }
                 isAllcolumns = false;
                 selectedColumns = new HashSet<String>();
                 setSelectedColumns();
        }



    }

    private void setSelectedColumns()throws SQLException
    {
        selectItems = ((PlainSelect)selectBody).getSelectItems();

        for(int i=0; i< selectItems.size(); i++)
        {
            SelectItem selectItem = selectItems.get(i);
            Expression expression = ((SelectExpressionItem)selectItem).getExpression();
            Utility.eval(expression,selectedColumns);
        }
        if(hasWhereCondition)
        {
            Utility.eval(whereCondition,selectedColumns);
        }


    }

    private void parseWhereCondition()
    {

        whereCondition = ((PlainSelect) selectBody).getWhere();

        if(whereCondition != null)
        {
            hasWhereCondition = true;

        }
        else {

            hasWhereCondition = false;
        }

    }

    public RowTraverser processQuery() throws IOException, SQLException,ClassNotFoundException
    {
        switch (selectType)
        {
            case PLAIN_SELECT: return executePlainSelectQuery();
            case UNION_SELECT: return  executeSelectUnionQuery();
        }

        return null;
    }



    private RowTraverser executePlainSelectQuery() throws IOException, SQLException,ClassNotFoundException
    {
        ProjectIterator projectIterator = null;



        switch (FromType)
        {
            case FROM_TABLE:

                RowTraverser rowIterator = getTableTraverser(TableName,isAllcolumns,hasTableAlias,LeftTableAlias,selectedColumns);


                if(hasWhereCondition)
                {
                    rowIterator = new FilterIterator(rowIterator,whereCondition);
                }

                if(isAggregate || hasGroupBy)
                {
                    rowIterator = new ProjectIterator( new AggregateIterator(rowIterator,selectItems,groupByColumns,inMemory),selectItems,true);

                }
                if(hasHavingCondition)
                {
                    rowIterator = new FilterIterator(rowIterator,havingCondition);
                }
                if(hasOrderBy)
                {
                    rowIterator = new OrderByIterator(rowIterator,orderByFields,inMemory);

                }
                if(hasLimitBy)
                {
                    rowIterator = new LimitIterator(rowIterator,limitValue);
                }
                 projectIterator = new ProjectIterator(rowIterator,selectItems,isAllcolumns);

                 break;

            case FROM_SUBSELECT:

                SelectProcessor subSelectProcessor = new SelectProcessor(subSelectBody,inMemory,queryAlias);

                RowTraverser DataRowIterator = subSelectProcessor.processQuery();

                if(hasWhereCondition)
                {
                    DataRowIterator = new FilterIterator(DataRowIterator,whereCondition);
                }
                if(isAggregate || hasGroupBy)
                {
                    DataRowIterator = new AggregateIterator(DataRowIterator,selectItems,groupByColumns,inMemory);
                }
                if(hasHavingCondition)
                {
                    DataRowIterator = new FilterIterator(DataRowIterator,havingCondition);
                }
                if(hasOrderBy)
                {
                    DataRowIterator = new OrderByIterator(DataRowIterator,orderByFields,inMemory);
                }
                if(hasLimitBy)
                {
                    DataRowIterator = new LimitIterator(DataRowIterator,limitValue);
                }

                projectIterator = new ProjectIterator(DataRowIterator,selectItems,isAllcolumns);

                break;

            case FROM_JOIN:

                if(hasWhereCondition)
                {
                    JoinIterator = new FilterIterator(JoinIterator,whereCondition);
                }

                if(isAggregate || hasGroupBy)
                {
                    JoinIterator = new AggregateIterator(JoinIterator,selectItems,groupByColumns,inMemory);

                }
                if(hasHavingCondition)
                {
                    JoinIterator = new FilterIterator(JoinIterator,havingCondition);
                }
                if(hasOrderBy)
                {
                    JoinIterator = new OrderByIterator(JoinIterator,orderByFields,inMemory);
                }
                if(hasLimitBy)
                {
                    JoinIterator = new LimitIterator(JoinIterator,limitValue);
                }

                projectIterator = new ProjectIterator(JoinIterator,selectItems,isAllcolumns);

                break;
        }

        if(hasQueryAlias)
        {
            projectIterator.updateProjectionMapping(queryAlias);
        }

        return projectIterator;

    }

    private RowTraverser executeSelectUnionQuery()throws IOException, SQLException, ClassNotFoundException
    {
        List<RowTraverser> IteratorsList = new ArrayList<RowTraverser>();

        for(PlainSelect select: PlainSelectList)
        {
            SelectProcessor selectProcessor = new SelectProcessor((SelectBody)select,inMemory,null);
            RowTraverser rowIterator = selectProcessor.processQuery();
            IteratorsList.add(rowIterator);
        }

        UnionIterator unionIterator = new UnionIterator(IteratorsList);

        return unionIterator;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {

            selectedColumns.add(column.toString());
            return null;

    }

    private RowTraverser getTableTraverser(String TableName,boolean isAllcolumns,boolean hasTableAlias,String LeftTableAlias, Set<String> selectedColumns)throws  IOException
    {
        RowTraverser rowIterator = null;

        dubstep.Table table = TableInformation.getTable(TableName);

        if(isAllcolumns)
        {
            rowIterator = new TupleTraverser(TableName,hasTableAlias,LeftTableAlias);

        }
        else
        {
            if(table.hasUpdateColumns())
            {
                table.addUpdateColumns(selectedColumns);
            }
            rowIterator = new SelectTraverser(TableName,selectedColumns,hasTableAlias,LeftTableAlias);

        }

        if(table.hasUpdates())
        {
            rowIterator = table.getUpdatedIterator(rowIterator,isAllcolumns);
        }

        return rowIterator;
    }

}
