package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.sql.SQLException;
import java.util.HashMap;

public interface Accumulator
{
   void Accumulate(PrimitiveValue[]dataRow, HashMap<String,Integer> fieldMapping) throws SQLException;
   PrimitiveValue Fold()throws SQLException;
}
