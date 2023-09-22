package org.apache.spark.sql.execution.datasources.jdbc

import java.lang.reflect.Method

/**
 * @author Pan Jiebin
 */
object ReflectHelper {

  def reflectMethod(obj: AnyRef, methodName: String, paramTypes: Class[_]*) : Method = {
    var method: Method = null
    try {
      method = obj.getClass.getDeclaredMethod(methodName, paramTypes: _*)
    } catch {
      case _: Throwable =>
    }
    if (method == null) {
      method = obj.getClass.getSuperclass.getDeclaredMethod(methodName, paramTypes: _*)
    }
    method
  }

  def invoke(obj: AnyRef, method: Method, params: Object*): Object = {
    var isSetAccessible = false
    if(!method.isAccessible) {
      method.setAccessible(true)
      isSetAccessible = true
    }
    val res = method.invoke(obj, params: _*)
    if (isSetAccessible) {
      method.setAccessible(false)
    }
    res
  }

  def reflectValue[T](obj: AnyRef, fieldName: String, isSuper: Boolean = false): T = {
    var clazz: Class[_] = obj.getClass
    if (isSuper) {
      clazz = clazz.getSuperclass
    }
    val field = clazz.getDeclaredField(fieldName)
    field.setAccessible(true)
    val value = field.get(obj)
    field.setAccessible(false)
    value.asInstanceOf[T]
  }
}
