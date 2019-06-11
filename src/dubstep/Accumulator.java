package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.sql.SQLException;
import java.util.HashMap;

public interface Accumulator
{
   void Accumulate(PrimitiveValue[]dataRow) throws SQLException;
   PrimitiveValue Fold()throws SQLException;
   void init(PrimitiveValue[] dataRow) throws SQLException;
}
