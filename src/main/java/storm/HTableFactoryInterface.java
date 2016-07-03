package storm;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface HTableFactoryInterface
{
    HTableInterface checkTable();
    HTableInterface createTable();
    
    /**
     * Cleanup any resources held opened during the table's lifetime
     * Call this after the closing the table
     */
    void cleanup();
}
