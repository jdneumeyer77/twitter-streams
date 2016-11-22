import scala.collection.concurrent.TrieMap

object Utils {
  implicit final class IncrDictionary(val map: TrieMap[String,Long]) extends AnyVal {
     def addOrIncr(key: String) = {
       map.get(key) match {
         case Some(value) => map.update(key, value+1)
         case None => map.update(key, 1)
       }
     }
  }
}
