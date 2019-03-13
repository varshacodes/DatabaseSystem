package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

public interface RowTraverser
{
    PrimitiveValue[] next()throws SQLException, IOException;
    boolean hasNext()throws IOException;
    void reset()throws IOException;
    HashMap<String, Integer> getFieldPositionMapping();
    void close() throws IOException;
}
