package dubstep;

import java.io.*;
import java.sql.SQLException;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class Main
{

    public static Statement statement;

    public static void main(String args[]) throws ParseException, IOException, SQLException, ClassNotFoundException
    {
        CCJSqlParser parser = new CCJSqlParser(System.in);
        TableWriter tableWriter = null;
        boolean inMemory = true;
        System.out.println("$> ");

        if(Utility.isTableInfoAvailable())
        {
            TableReader tableReader = new TableReader();
            tableReader.LoadTableInfo();
            tableWriter = new TableWriter();
        }
        else
        {
            tableWriter = new TableWriter();
        }

        statement = parser.Statement();

        while (statement != null)
        {
            if (statement instanceof Select)
            {
                SelectProcessor  selectProcessor = new SelectProcessor(((Select) statement).getSelectBody(),inMemory,null);
                RowTraverser RowIterator = selectProcessor.processQuery();
                if(selectProcessor.isOptimizable()) {
                    Optimizer optimizer = new Optimizer(RowIterator, inMemory);
                    RowIterator = optimizer.optimize();
                }
                printResult(RowIterator);

            } else if(statement instanceof CreateTable)
            {
                Table table = new Table((CreateTable)statement);
                TableInformation.addTableInfo(table);
                TableRowsWriter tableRowsWriter = new TableRowsWriter(table);
                Long rows = tableRowsWriter.writeTable(table);
                tableRowsWriter.close();
                table.setRows(rows);
                tableWriter.writeTable(table);
            }
            else if(statement instanceof Insert)
            {
               InsertProcessor insertProcessor = new InsertProcessor((Insert) statement);
            }
            else if(statement instanceof Delete)
            {
                DeleteProcessor deleteProcessor = new DeleteProcessor((Delete) statement);
            }
            else if(statement instanceof Update)
            {
                UpdateProcessor updateProcessor = new UpdateProcessor((Update) statement);
            }

            System.out.println("$> ");
            statement =  parser.Statement();

        }


    }

    public static void printResult(RowTraverser rowIterator)throws SQLException,IOException,ClassNotFoundException
    {
        while (rowIterator != null)
        {
            PrimitiveValue[]  dataRow = rowIterator.next();

            if(dataRow != null && dataRow.length!=0)
            {
                System.out.print(Utility.getLine(dataRow));
            }
            else
            {
                return;
            }
        }
        rowIterator.close();
    }
}