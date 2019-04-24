package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
    Iterator<PrimitiveValue[]> resultIterator;
    boolean isInMemory;
    HashMap<String,Integer> rowIteratorMapping;
    boolean isInitialized;

    AggregateIterator(RowTraverser rowIterator, List<SelectItem> selectItems,List<Column> groupBy,boolean inMemory)
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
            ArrayList<Accumulator> accumulators = getAccumulators();
            ArrayList<PrimitiveValue[]> result = new ArrayList<PrimitiveValue[]>();

            while (dataRow != null)
            {
                for (Accumulator accumulator: accumulators)
                {
                    accumulator.Accumulate(dataRow,rowIteratorMapping);
                }
                dataRow = rowIterator.next();
            }

            PrimitiveValue arr[] = new PrimitiveValue[accumulators.size()];

            for(int i=0; i < accumulators.size(); i++)
            {
                arr[i] = accumulators.get(i).Fold();

            }
            result.add(arr);
            this.resultIterator = result.iterator();
        }

    }

    private void computeRows()throws IOException, SQLException, ClassNotFoundException
    {
        PrimitiveValue[] dataRow = rowIterator!=null?rowIterator.next(): null;

        if(dataRow!=null)
        {
            int []groupByPos = getGroupByColumnPositions();
            HashMap<ArrayList<PrimitiveValue>,ArrayList<Accumulator>> resultmap = new  HashMap<>();

            while (dataRow != null)
            {
                ArrayList<PrimitiveValue> groupByFields= getGroupByColumns(dataRow,groupByPos);
                ArrayList<Accumulator> accumulators = resultmap.getOrDefault(groupByFields,getAccumulators());

                for(Accumulator accumulator: accumulators)
                {
                    accumulator.Accumulate(dataRow,rowIteratorMapping);
                }

                resultmap.put(groupByFields,accumulators);

                dataRow = rowIterator.next();
            }
            ArrayList<PrimitiveValue[]> result = getResult(resultmap);
            this.resultIterator = result.iterator();
        }

    }

    private ArrayList<PrimitiveValue[]> getResult(HashMap<ArrayList<PrimitiveValue>,ArrayList<Accumulator>> resultmap)throws SQLException
    {
        Iterator<ArrayList<PrimitiveValue>> keyIterator = resultmap.keySet().iterator();
        ArrayList<PrimitiveValue[]> dataRows = new ArrayList<>();

        while(keyIterator.hasNext())
        {
            ArrayList<PrimitiveValue> groupByColumns = keyIterator.next();
            ArrayList<Accumulator> accumulators = resultmap.get(groupByColumns);
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
                    dataRow[i] = accumulators.get(accumulator++).Fold();
                }
            }
            dataRows.add(dataRow);
        }

        return dataRows;
    }

    private ArrayList<PrimitiveValue> getGroupByColumns(PrimitiveValue[] dataRow,int []positions)
    {
        ArrayList<PrimitiveValue> groupBy = new ArrayList<PrimitiveValue>();

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


    private ArrayList<Accumulator> getAccumulators()
    {
        ArrayList<Accumulator> accumulators = new ArrayList<Accumulator>();

        for(int i=0; i < aggregateFields.size(); i++)
        {
            AggregateField aggregateField = aggregateFields.get(i);
            Expression expression = aggregateField.getExpression();

            switch (aggregateField.getAggregate())
            {
                case AVG: accumulators.add(new AverageAccumulator(expression)); break;
                case MAX: accumulators.add(new MaxAccumulator(expression)); break;
                case MIN: accumulators.add(new MinAccumulator(expression)); break;
                case COUNT: accumulators.add(new CountAccumulator(expression)); break;
                case SUM: accumulators.add(new SumAccumulator(expression)); break;
            }

        }
        return  accumulators;
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

    private void parseSelectItems(List<SelectItem> selectItems)
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
                    aggregateFields.add(new AggregateField(aggregate,null));
                    isSelectColumn.add(false);
                } else {
                    Aggregate aggregate = Aggregate.getAggregate(function.getName());
                    Expression expression = function.getParameters().getExpressions().get(0);
                    aggregateFields.add(new AggregateField(aggregate, expression));
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
            String Alias= selectItem.getAlias();

            if(Alias == null)
            {
                Alias = selectItem.toString();

            }

            fieldMapping.put(Alias,i);
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
        if(isInMemory || !isGroupBy)
        {
            if(resultIterator !=null && resultIterator.hasNext())
            {
                PrimitiveValue[] dataRow = resultIterator.next();
                return dataRow;
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
        ArrayList<Accumulator> accumulators = getAccumulators();

        while (current!=null && groupBy.equals(getGroupByColumns(current,groupByPos)))
        {
            for(Accumulator accumulator: accumulators)
            {
                accumulator.Accumulate(current,rowIteratorMapping);
            }
            current = sortedIterator.next();
        }

        return getResult(groupBy,accumulators);
    }

    private PrimitiveValue[] getResult(ArrayList<PrimitiveValue> groupBy, ArrayList<Accumulator> accumulators)throws SQLException
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
                    dataRow[i] = accumulators.get(accumulator++).Fold();
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

    @Override
    public PrimitiveValue[] getcurrent()
    {
       return current;
    }

    public RowTraverser getChild()
    {
        return rowIterator;
    }

}

