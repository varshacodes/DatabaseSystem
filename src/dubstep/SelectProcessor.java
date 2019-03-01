package dubstep;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import java.io.IOException;


public class SelectProcessor
{
    public static final int FROM_TABLE = 201;
    public static final int FROM_JOIN = 202;
    public static final int FROM_SUBSELECT = 203;
    public static final int PLAIN_SELECT= 301;
    public static final int UNION_SELECT = 302;

    SelectBody selectBody;
    FromItem fromItem;
    String TableName;
    SelectBody subSelectBody;
    boolean isAllcolumns;
    List<SelectItem> selectItems;
    boolean hasWhereCondition;
    Expression whereCondition;
    FromItem rightItem;
    String rightTable;
    List<PlainSelect> PlainSelectList;
    boolean hasTableAlias;
    String LeftTableAlias;
    String RightTableAlias;

    int selectType;
    int FromType;

    public SelectProcessor(SelectBody selectBody)
    {
       this.selectBody = selectBody;
       parseSelectBody();
    }

    public String getTableName()
    {
       return TableName;
    }

    private void parseSelectBody()
    {
        if(selectBody instanceof PlainSelect)
        {
            selectType = PLAIN_SELECT;
            parseFromItem();
            parseSelectItem();
            parseWhereHavingCondition();

        }
        else if(selectBody instanceof Union)
        {
          selectType = UNION_SELECT;
          PlainSelectList = ((Union) selectBody).getPlainSelects();

        }
    }

    private void parseFromItem()
    {
        fromItem = ((PlainSelect)selectBody).getFromItem();
        List<Join> joins= ((PlainSelect) selectBody).getJoins();


        if(fromItem instanceof Table)
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
        } 
        if (joins != null)
        {
            FromType = FROM_JOIN;
            for (Join join : joins)
            {
                rightItem = join.getRightItem();
                if (rightItem instanceof  Table)
                {
                    rightTable = ((Table) rightItem).getWholeTableName();

                    if(rightItem.getAlias() !=null)
                    {
                        RightTableAlias = rightItem.getAlias();
                    }
                }
            }

        }

    }

    private void parseSelectItem()
    {

        selectItems = ((PlainSelect)selectBody).getSelectItems();
        SelectItem selectItem = selectItems.iterator().next();

        if((selectItem instanceof AllColumns)|| (selectItem instanceof  AllTableColumns))
        {
            isAllcolumns = true;
        }
        else {

            isAllcolumns = false;

            if((FromType != FROM_SUBSELECT)&&(selectItem.toString().contains(TableName+".")))
            {
                hasTableAlias = true;
                LeftTableAlias = TableName;
                RightTableAlias = rightTable;
            }

        }

    }
    private void parseWhereHavingCondition()
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

    public RowTraverser processQuery() throws IOException, SQLException
    {
        switch (selectType)
        {
            case PLAIN_SELECT: return executePlainSelectQuery();
            case UNION_SELECT: return  executeSelectUnionQuery();
        }

        return null;
    }



    private RowTraverser executePlainSelectQuery() throws IOException, SQLException
    {
        HashMap<String,Integer> fieldPositionMapping;
        HashMap<String,Integer> rightMapping;

        if(hasTableAlias)
        {
            fieldPositionMapping = TableInformation.getFieldMappingwithAlias(TableName,LeftTableAlias);
        }
        else
        {
            fieldPositionMapping = TableInformation.getFieldPostionMapping(TableName);

        }

        switch (FromType)
        {
            case FROM_TABLE:

                RowIterator rowIterator = new RowIterator(TableName,fieldPositionMapping);

                if(isAllcolumns)
                {

                    if(hasWhereCondition)
                    {
                        FilterIterator filterIterator = new FilterIterator(rowIterator,whereCondition);
                        return filterIterator;
                    }
                    else
                    {
                        return rowIterator;
                    }
                }
                else
                {

                     if(hasWhereCondition)
                     {
                         FilterIterator filterIterator = new FilterIterator(rowIterator,whereCondition);
                         ProjectIterator projectIterator = new ProjectIterator(filterIterator,selectItems);
                         return projectIterator;
                     }
                     else
                     {
                         ProjectIterator projectIterator = new ProjectIterator(rowIterator,selectItems);
                         return projectIterator;
                     }


                }

            case FROM_SUBSELECT:

                SelectProcessor subSelectProcessor = new SelectProcessor(subSelectBody);
                RowTraverser DataRowIterator = subSelectProcessor.processQuery();

                    if(isAllcolumns)
                    {
                        if(hasWhereCondition)
                        {
                            FilterIterator filterIterator = new FilterIterator(DataRowIterator,whereCondition);
                            return  filterIterator;
                        }
                        else
                        {
                            return DataRowIterator;
                        }

                    }
                    else {
                            if(hasWhereCondition)
                            {
                                FilterIterator filterIterator = new FilterIterator(DataRowIterator,whereCondition);
                                ProjectIterator projectIterator = new ProjectIterator(filterIterator,selectItems);
                                return projectIterator;

                            }
                            else {

                                ProjectIterator projectIterator = new ProjectIterator(DataRowIterator,selectItems);
                                return projectIterator;
                            }
                    }

            case FROM_JOIN:

                RowIterator left = new RowIterator(TableName,fieldPositionMapping);
                if(hasTableAlias)
                {
                    rightMapping = TableInformation.getFieldMappingwithAlias(TableName, RightTableAlias);
                }
                else{

                    rightMapping = TableInformation.getFieldPostionMapping(rightTable);
                }

                RowIterator right = new RowIterator(rightTable,rightMapping);
                JoinIterator joinIterator = new JoinIterator(left,right);

                if (isAllcolumns)
                {
                    if(hasWhereCondition)
                    {
                        FilterIterator filterIterator = new FilterIterator(joinIterator,whereCondition);
                        return filterIterator;
                    }
                    else
                    {
                        return joinIterator;
                    }
                }
                else
                {
                    if(hasWhereCondition)
                    {
                        FilterIterator filterIterator = new FilterIterator(joinIterator,whereCondition);
                        ProjectIterator projectIterator = new ProjectIterator(filterIterator,selectItems);
                        return projectIterator;

                    }
                    else
                    {
                        ProjectIterator projectIterator = new ProjectIterator(joinIterator,selectItems);
                        return projectIterator;
                    }
                }


        }

        return null;

    }

    private RowTraverser executeSelectUnionQuery()throws IOException, SQLException
    {
        List<RowTraverser> IteratorsList = new ArrayList<RowTraverser>();

        for(PlainSelect select: PlainSelectList)
        {
            SelectProcessor selectProcessor = new SelectProcessor((SelectBody)select);
            RowTraverser rowIterator = selectProcessor.processQuery();
            IteratorsList.add(rowIterator);
        }

        UnionIterator unionIterator = new UnionIterator(IteratorsList);

        return unionIterator;

    }
}
