package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class AggregateIterator implements RowTraverser
{
    RowTraverser rowIterator;
    HashMap<String,Integer> fieldMapping;
    List<Boolean> isSelectColumn;
    List<AggregateField> aggregateFields;
    List<Field> groupBy;
    List<Field> selectFields;
    boolean isGroupBy;
    HashMap<String,Integer> groupByMapping;
    private PrimitiveValue[] current;
    RowTraverser sortedIterator;
    Iterator<ArrayList<PrimitiveValue>> resultIterator;
    Iterator<PrimitiveValue[]> result;
    boolean isInMemory;
    HashMap<String,Integer> rowIteratorMapping;
    boolean isInitialized;
    HashMap<ArrayList<PrimitiveValue>,Accumulator[]> resultmap;

    AggregateIterator(RowTraverser rowIterator, List<SelectItem> selectItems,List<Column> groupBy,boolean inMemory)throws SQLException
    {
        this.rowIterator = rowIterator;
        this.isInMemory = inMemory;
        setFieldPositionMapping(selectItems);
        this.rowIteratorMapping = rowIterator.getFieldPositionMapping();
        this.isInitialized = false;
        if(groupBy !=null)
        {
            isGroupBy = true;
            parseGroupBy(groupBy);
        }
        else
        {
            isGroupBy = false;
        }
        parseSelectItems(selectItems);


    }


    @Override
    public int getNoOfFields()
    {
        return selectFields.size() + aggregateFields.size();
    }


    public void setRowIterator(RowTraverser rowIterator)
    {
        this.rowIterator = rowIterator;
    }

    private void initialize()throws SQLException,IOException,ClassNotFoundException
    {
        if(!isInMemory && isGroupBy)
        {
            sortedIterator = new SortOnDisk(rowIterator,this.groupBy,"GroupBy");
            current = sortedIterator !=null ? sortedIterator.next(): null;
        }

        if(!isGroupBy)
        {
            computeRow();
        }
        else if(isInMemory)
        {
            computeRows();
        }

    }

    private void computeRow() throws IOException,SQLException,ClassNotFoundException
    {
        PrimitiveValue[] dataRow = rowIterator != null ? rowIterator.next(): null;

        if(dataRow!=null)
        {
            Accumulator[] accumulators = getAccumulators(dataRow);
            dataRow = rowIterator.next();

            while (dataRow != null)
            {
                for (Accumulator accumulator: accumulators)
                {
                    accumulator.Accumulate(dataRow);
                }
                dataRow = rowIterator.next();
            }

             PrimitiveValue[] result = new PrimitiveValue[accumulators.length];

            for(int i=0; i < accumulators.length; i++)
            {
                result[i] = accumulators[i].Fold();

            }
            ArrayList<PrimitiveValue[]> resultSet = new ArrayList<>();
            resultSet.add(result);
            this.result = resultSet.iterator();

        }

    }

    private void computeRows()throws IOException, SQLException, ClassNotFoundException
    {
        PrimitiveValue[] dataRow = rowIterator!=null?rowIterator.next(): null;

        if(dataRow!=null)
        {
            int []groupByPos = getGroupByColumnPositions();
            this.resultmap = new  HashMap<>();

            while (dataRow != null)
            {
                ArrayList<PrimitiveValue> groupByFields= getGroupByColumns(dataRow,groupByPos);
                if(resultmap.containsKey(groupByFields))
                {
                    Accumulator[] accumulators = resultmap.get(groupByFields);
                    for(Accumulator accumulator: accumulators)
                    {
                        accumulator.Accumulate(dataRow);
                    }
                }
                else
                {
                    Accumulator[] accumulators = getAccumulators(dataRow);
                    resultmap.put(groupByFields,accumulators);
                }
                dataRow = rowIterator.next();
            }
            this.resultIterator = resultmap.keySet().iterator();
        }

    }

    private PrimitiveValue[] getResult(ArrayList<PrimitiveValue> groupByColumns)throws SQLException
    {
            Accumulator[] accumulators = resultmap.get(groupByColumns);
            PrimitiveValue[] dataRow = new PrimitiveValue[isSelectColumn.size()];
            int accumulator =0;
            int selectfield = 0;
            for(int i=0; i < isSelectColumn.size(); i++)
            {
                if(isSelectColumn.get(i).booleanValue())
                {
                    dataRow[i] = groupByColumns.get(selectFields.get(selectfield++).getPosition());
                }
                else
                {
                    dataRow[i] = accumulators[accumulator++].Fold();
                }
            }
        return dataRow;
    }

    private ArrayList<PrimitiveValue> getGroupByColumns(PrimitiveValue[] dataRow,int []positions)
    {
        ArrayList<PrimitiveValue> groupBy = new ArrayList<PrimitiveValue>(positions.length);

        for(int i=0; i < positions.length; i++)
        {
            groupBy.add(dataRow[positions[i]]);

        }
        return groupBy;
    }

    private int[] getGroupByColumnPositions()
    {
        int pos[] = new int[groupBy.size()];

        for(int i =0; i < groupBy.size(); i++)
        {
            pos[i] = groupBy.get(i).getPosition();
        }
        return pos;
    }


    private Accumulator[] getAccumulators(PrimitiveValue[] dataRow) throws SQLException
    {
        Accumulator[] accumulators = new Accumulator[aggregateFields.size()];

        for(int i=0; i < aggregateFields.size(); i++)
        {
            AggregateField aggregateField = aggregateFields.get(i);
            accumulators[i] =aggregateField.getNewAccumulator(dataRow);

        }
        return accumulators;
    }
    private void parseGroupBy(List<Column> groupByList)
    {
        this.groupBy = new ArrayList<>();
        HashMap<String,Integer> fieldmapping = rowIterator.getFieldPositionMapping();
        this.groupByMapping = new HashMap<String,Integer>();
        for(int i=0; i< groupByList.size(); i++)
        {
            Column column = groupByList.get(i);
            groupBy.add(new Field(column.toString(),fieldmapping.get(column.toString()),null));
            groupByMapping.put(column.toString(),i);
        }
    }

    private void parseSelectItems(List<SelectItem> selectItems)throws SQLException
    {
        selectFields = new ArrayList<Field>();
        aggregateFields = new ArrayList<AggregateField>();
        isSelectColumn =  new ArrayList<Boolean>();

        for(int i=0; i< selectItems.size(); i++)
        {
            Expression selectItem = ((SelectExpressionItem)selectItems.get(i)).getExpression();
            if(selectItem instanceof Function)
            {
                Function function = ((Function) selectItem);

                if (function.isAllColumns())
                {
                    Aggregate aggregate = Aggregate.getAggregate(function.getName());
                    aggregateFields.add(new AggregateField(aggregate,null,null));
                    isSelectColumn.add(false);
                } else {
                    Aggregate aggregate = Aggregate.getAggregate(function.getName());
                    Expression expression = function.getParameters().getExpressions().get(0);
                    aggregateFields.add(new AggregateField(aggregate, expression,rowIteratorMapping));
                    isSelectColumn.add(false);
                }
            }
            else if(selectItem instanceof Column)
            {
                String column = ((Column) selectItem).toString();
                if(groupBy == null) { groupByMapping = rowIterator.getFieldPositionMapping(); }
                int groupPosition = groupByMapping.get(column);
                selectFields.add(new Field(column,groupPosition,null));
                isSelectColumn.add(true);
            }

        }

        if(!this.isGroupBy)
        {
            selectFields = null;
            isSelectColumn = null;
        }
    }


    private void setFieldPositionMapping(List<SelectItem> selectItems)
    {
        fieldMapping = new HashMap<String,Integer>();

        for(int i=0; i < selectItems.size(); i++)
        {
            SelectExpressionItem selectItem = (SelectExpressionItem) selectItems.get(i);
            if(selectItem.getAlias()!=null)
            {
                fieldMapping.put(selectItem.getAlias(),i);
            }
            else {
                Expression expression = ((SelectExpressionItem) selectItems.get(i)).getExpression();

                if ((expression instanceof Column) && (((Column) expression).getTable() != null)) {
                    fieldMapping.put(selectItem.toString(), i);
                    fieldMapping.put(((Column) expression).getColumnName(), i);
                }
            }
            fieldMapping.put(selectItem.toString(),i);
        }

    }

    @Override
    public PrimitiveValue[] next() throws SQLException, IOException,ClassNotFoundException
    {
        if(!isInitialized)
        {
            initialize();
            isInitialized = true;
        }

        if(!isGroupBy && result.hasNext())
        {
            return result.next();

        }
        else if(isInMemory && isGroupBy)
        {
            if(resultIterator !=null && resultIterator.hasNext())
            {
                return  getResult(resultIterator.next());
            }
        }
        else {
                 if(current !=null)
                 {
                     PrimitiveValue[] datarow = getGroupResult();
                     return datarow;
                 }
        }

        return null;
    }

    private PrimitiveValue[] getGroupResult() throws SQLException,IOException,ClassNotFoundException
    {
        int groupByPos[] = getGroupByColumnPositions();
        ArrayList<PrimitiveValue> groupBy = getGroupByColumns(current,groupByPos);
        Accumulator[] accumulators = getAccumulators(current);
        current = sortedIterator.next();

        while (current!=null && groupBy.equals(getGroupByColumns(current,groupByPos)))
        {
            for(Accumulator accumulator: accumulators)
            {
                accumulator.Accumulate(current);
            }
            current = sortedIterator.next();
        }

        return getResult(groupBy,accumulators);
    }

    private PrimitiveValue[] getResult(ArrayList<PrimitiveValue> groupBy,Accumulator[] accumulators)throws SQLException
    {
        PrimitiveValue[] dataRow = new PrimitiveValue[isSelectColumn.size()];
        int accumulator =0;
        int selectfield =0;
        for(int i=0; i < isSelectColumn.size(); i++)
        {

            if(isSelectColumn.get(i).booleanValue())
            {
                    dataRow[i] = groupBy.get(selectFields.get(selectfield++).getPosition());
            }
            else {
                    dataRow[i] = accumulators[accumulator++].Fold();
            }
        }
        return dataRow;
    }



    @Override
    public void reset() throws IOException, SQLException,ClassNotFoundException
    {
      rowIterator.reset();
      isInitialized = false;
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return fieldMapping;
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
       rowIterator.close();
    }


    public RowTraverser getChild()
    {
        return rowIterator;
    }

}

