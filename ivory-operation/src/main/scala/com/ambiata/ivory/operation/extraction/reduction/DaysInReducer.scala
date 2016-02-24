package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Date
/**
 * Count the amount of time spent in each state within a given window.
 * If one reenters a state, the additional time will be grouped into
 * one accumulation. Time spent in no-value (tombstoned) states will
 * not be allocated into any window.
 */

case class DaysInState[A](var lastVal: A, var lastDate: Int, var lastTomb: Boolean, var state: KeyValue[A, Int])

class DaysInReducer[A](dates: DateOffsets, empty: A) extends ReductionFoldWithDate[DaysInState[A], A, KeyValue[A, Int]] {
  def initial: DaysInState[A] =
    DaysInState(empty, -1, true, new KeyValue[A, Int])

  def foldWithDate(s: DaysInState[A], y: A, date: Date): DaysInState[A] = {
    // Get the days offset for this fact.
    // State based facts may `see` one value before the window as well,
    // but, this will be recorded at the very beginning of the window due to
    // DateOffsets having a minimum at the start of the window. This means that
    // the maximum time for DaysInReducer is the length of the windows.
    updateState(s, date)
    // Add information about this fact for the next time around.
    s.lastVal  = y
    s.lastTomb = false
    s
  }

  def tombstoneWithDate(s: DaysInState[A], date: Date): DaysInState[A] = {
    // We need to do work when a tombstone comes along. The previous fact
    // was true until now, so add it to the map with its length of time
    // up until this tombstone was dropped.
    updateState(s, date)
    // Record that this was a tombstone for the next time around.
    s.lastTomb = true
    s
  }

  def aggregate(s: DaysInState[A]): KeyValue[A, Int] = {
    // There are no more facts, but we still haven't put the most recent one into
    // the map with time up until the end of the window.
    updateState(s, dates.end)
    // This isn't strictly necessary, but it makes aggregate idempotent.
    s.lastTomb = true
    s.state
  }

  def updateState(s: DaysInState[A], date: Date): Unit = {
    val xDate = dates.get(date).value
    // If the last fact was not a tombstone (was a real value), then we now know
    // how long it has been active for, and can add it to the key value store.
    if (!s.lastTomb) {
      val old = s.state.getOrElse(s.lastVal, 0)
      // Add its previous time to its new time, (which is the time diff between
      // when it was set and now).
      s.state.put(s.lastVal, old + (xDate - s.lastDate))
    }
    s.lastDate = xDate
  }
}
