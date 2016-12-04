import java.lang.Math._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import com.codahale.metrics.{Clock, EWMA}

object Metrics {
  // Needed for EWMA.
  private val INTERVAL: Int = 5
  private val SECONDS_PER_MINUTE: Double = 60.0
  private val SIXTY_MINUTES = 60
  private val M60_ALPHA: Double = 1 - exp(-INTERVAL / SECONDS_PER_MINUTE / SIXTY_MINUTES)
  private val TICK_INTERVAL: Long = TimeUnit.SECONDS.toNanos(INTERVAL)

  // Based on Dropwizard's meter, but it wasn't fit to be extended...
  class CustomMeter {
    import java.util.concurrent.TimeUnit
    private val m1Rate: EWMA = EWMA.oneMinuteEWMA
    private val m60Rate = new com.codahale.metrics.EWMA(M60_ALPHA, INTERVAL, TimeUnit.SECONDS)

    private val counter = new LongAdder()
    private val clock = Clock.defaultClock()
    private val startTime = clock.getTick
    private val lastTick = new AtomicLong(startTime)

    def mark(): Unit = {
      mark(1)
    }

    def mark(n: Long): Unit = {
      tickIfNecessary()
      counter.add(n)
      m1Rate.update(n)
      m60Rate.update(n)
    }

    private def tickIfNecessary() {
      val oldTick = lastTick.get
      val newTick = clock.getTick
      val age = newTick - oldTick
      if (age > TICK_INTERVAL) {
        val newIntervalStartTick = newTick - age % TICK_INTERVAL
        if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
          val requiredTicks: Long = age / TICK_INTERVAL
          (0L until requiredTicks).foreach { _ =>
            m1Rate.tick()
            m60Rate.tick()
          }
        }
      }
    }

    def sixtyMinuteRate() = {
      tickIfNecessary()
      m60Rate.getRate(TimeUnit.SECONDS)
    }

    def oneMinuteRate() = {
      tickIfNecessary()
      m1Rate.getRate(TimeUnit.SECONDS)
    }

    def count = counter.sum()

    def meanRate = if (count == 0) 0.0
    else {
      val elapsed: Double = clock.getTick - startTime
      count / elapsed * TimeUnit.SECONDS.toNanos(1)
    }
  }

  def meter: CustomMeter = new CustomMeter
}
