package edu.ucr.cs.cs167.Wildfire_Analysis_Group_C4;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // Read in the parquet file created above
// Parquet files are self-describing so the schema is preserved
// The result of loading a Parquet file is also a DataFrame
        val parquetFileDF = spark.read.parquet("people.parquet")
        System.out.println( "Hello World!" );
    }
}
