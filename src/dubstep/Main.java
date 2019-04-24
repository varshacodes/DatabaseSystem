package dubstep;

import java.io.*;
import java.sql.SQLException;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class Main
{

    public static Statement statement;

    public static void main(String args[]) throws ParseException, IOException, SQLException, ClassNotFoundException
    {
        CCJSqlParser parser = new CCJSqlParser(System.in);
        boolean inMemory = true;
        if(args.length > 0 && args[0].equals("--on-disk"))
        {
            inMemory = false;
        }
        else
        {
            inMemory = true;
        }
        System.out.println("$> ");

        while ((statement = parser.Statement()) != null)
        {
            if (statement instanceof Select)
            {
                SelectProcessor  selectProcessor = new SelectProcessor(((Select) statement).getSelectBody(),inMemory,null);
                RowTraverser RowIterator = selectProcessor.processQuery();
                Optimizer optimizer = new Optimizer(RowIterator, inMemory);
                RowIterator = optimizer.optimize();
                printResult(RowIterator);

            } else if (statement instanceof CreateTable)
            {
                Table table = new Table((CreateTable)statement);
                TableInformation.addTableInfo(table);
                TupleWriter tupleWriter = new TupleWriter(table);
                tupleWriter.writeTable(table);
                tupleWriter.close();
            }

            System.out.println("$> ");
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