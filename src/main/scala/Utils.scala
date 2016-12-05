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

    def countOf(key: String) = {
      map.getOrElse(key.toLowerCase, -1L)
    }

    def top(n: Int) = {
      map.readOnlySnapshot().toVector.sortBy(_._2)(reverseLongOrdering).take(n)
    }
  }

  private val simpleUnescape = Pattern.compile("""\""", Pattern.LITERAL)
  def domainFrom(expandedUrl: String): Try[String] = {
    Try(new URL(simpleUnescape.matcher(expandedUrl).replaceAll("")).getHost.toLowerCase)
  }
}
