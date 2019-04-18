package stream_outliers

/**
  * Created by simonyu on 2018-07-09.
  */
abstract class Result[T]

case class TruePositive[T](value: T) extends Result[T]

case class TrueNegative[T](value: T) extends Result[T]

case class FalsePositive[T](value: T) extends Result[T]

case class FalseNegative[T](value: T) extends Result[T]

case class Summary[T](var tp: Int = 0, var fp: Int = 0, var fn: Int = 0, var tn: Int = 0) {

  def + (that: Result[T]): Summary[T] = that match {
    case TruePositive(_) => new Summary[T](this.tp + 1, this.fp, this.fn, this.tn)
    case TrueNegative(_) => new Summary[T](this.tp, this.fp, this.fn, this.tn + 1)
    case FalsePositive(_) => new Summary[T](this.tp, this.fp + 1, this.fn, this.tn)
    case FalseNegative(_) => new Summary[T](this.tp, this.fp, this.fn + 1, this.tn)
  }

  def sensitivity: Float = tp.toFloat / (tp + fn)

  def specificity: Float = tn.toFloat / (tn + fp)

  def FNR: Float = 1 - sensitivity

  def FPR: Float = 1 - specificity

  def prevalence: Float = (tp + fn).toFloat / (tp + fp + fn + tn)

  def accuracy: Float = (tp + tn).toFloat / (tp + fp + fn + tn)

  def precision: Float = tp.toFloat / (tp + fp)

  def f_score: Float = 2 * precision * sensitivity / (precision + sensitivity)

  override def toString = s"True positives: $tp, False positives: $fp, True negatives: $tn, False negatives: $fn"

}