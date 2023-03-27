package simpledatalabs_27.sfefef

import org.apache.spark.sql._
package object functions {
  val vector_to_array  = Vector_to_array.vector_to_array
  val generate_vector  = Generate_vector.generate_vector
  val generate_matrix  = Generate_matrix.generate_matrix
  val matrix_col_count = Matrix_col_count.matrix_col_count

  def registerFunctions(spark: SparkSession) = {
    spark.udf.register("vector_to_array",  vector_to_array)
    spark.udf.register("generate_vector",  generate_vector)
    spark.udf.register("generate_matrix",  generate_matrix)
    spark.udf.register("matrix_col_count", matrix_col_count)
  }

}
