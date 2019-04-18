package stream_outliers


/**
  * Created by simonyu on 2018-06-10.
  */
case class ResultSummary[T](val points: Iterable[T], val tp: Int, val fp: Int, val fn: Int, val tn: Int) {

  def this() = this(Nil, 0, 0, 0, 0)

//  def +[T](that: ResultSummary[T]): ResultSummary[T] = new ResultSummary(
//    this.points ++ that.points,
//    this.tp + that.tp, this.fp + that.fp, this.fn + that.fn, this.tn + that.tn
//  )

  def + (that: ResultSummary[T]) = new ResultSummary[T](
    this.points.seq ++ that.points.seq,
    this.tp + that.tp,
    this.fp + that.fp,
    this.fn + that.fn,
    this.tn + that.tn)

  def sensitivity: Float = tp.toFloat / (tp + fn)

  def specificity: Float = tn.toFloat / (tn + fp)

  def FNR: Float = 1 - sensitivity

  def FPR: Float = 1 - specificity

  def prevalence: Float = (tp + fn).toFloat / (tp + fp + fn + tn)

  def accuracy: Float = (tp + tn).toFloat / (tp + fp + fn + tn)

  def precision: Float = tp.toFloat / (tp + fp)

  def f_score: Float = 2 * sensitivity * sensitivity / (sensitivity + sensitivity)

  override def toString = s"True positives: $tp, False positives: $fp, True negatives: $tn, False negatives: $fn"


}
