import java.net.URL
import java.util.regex.Pattern

import scala.collection.concurrent.TrieMap
import scala.util.Try

object Utils {
  private val reverseLongOrdering = Ordering.Long.reverse

  implicit final class IncrDictionary(val map: TrieMap[String,Long]) extends AnyVal {
    def addOrIncr(key: String) = {
       val keyLowered = key.toLowerCase
       map.get(keyLowered) match {
         case Some(value) => map.update(keyLowered, value+1)
         case None => map.update(keyLowered, 1)
       }
    }

    def valueOf(key: String) = {
      map.getOrElse(key.toLowerCase, 0L)
    }

    def top(n: Int) = {
      map.readOnlySnapshot().toVector.sortBy(_._2)(reverseLongOrdering).take(n)
    }
  }

  private val simpleUnexscape = Pattern.compile("""\""", Pattern.LITERAL)
  def domainFrom(expandedUrl: String): Try[String] = {
    Try(new URL(simpleUnexscape.matcher(expandedUrl).replaceAll("")).getHost.toLowerCase)
  }
}
