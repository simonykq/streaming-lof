package stream_outliers

import breeze.linalg._

/**
  * Created by simonyu on 2018-07-09.
  */


object Point {

  def apply(str: String): TPoint = new TPoint(
    DenseVector[Double](str.split(",").dropRight(1).map(_.toDouble)),
    str.split(",").last.toInt
  )

}

abstract class Point(val attrs: DenseVector[Double]) {

  protected var _k_distance: Double = 0
  protected var _lrd: Double = 0

  // Getter Setter
  def k_distance = _k_distance
  def k_distance_= (value: Double): Unit = _k_distance = value
  def lrd = _lrd
  def lrd_= (value: Double): Unit = _lrd = value

  def dist(that: Point): Double = Math.sqrt(sum((this.attrs - that.attrs).map(Math.pow(_, 2))))

  def k_nn[T <: Point](points: Iterable[T], k: Int) = points.par.map { pt =>
    (pt, this.dist(pt))
  }.toIndexedSeq.sortBy(_._2).take(k)

}

class TPoint(val value: DenseVector[Double],
             val label: Int) extends Point(value) {

  var cluster_id: Option[Long] = None

  def calculate_lof(points: Iterable[Point], k: Int): Double = (k_nn(points, k).map(_._1.lrd).sum / k) / this.lrd

  def calculate_lrd(points: Iterable[Point], k: Int): Unit = this.lrd = 1.0 / k_nn(points, k).map { pt_d =>
    Math.max(pt_d._2, pt_d._1.k_distance)
  }.sum / k

  def calculate_k_distance(points: Iterable[Point], k: Int): Unit = this.k_distance = k_nn(points, k)(k - 1)._2

}

class VPoint(var value: DenseVector[Double], val lambda: Double) extends Point(value) {

  var count: Int = 1
  var rank: Int = 0

  def merge(that: TPoint): Unit = {
    this.value = (this.value.map(_ * this.count) + that.value).map(_ / (this.count + 1))
    this.k_distance = (this._k_distance * this.count + that.k_distance) / (this.count + 1)
    this.lrd =  (this._lrd * this.count + that.lrd) / (this.count + 1)
    this.count += 1
  }

  override def k_distance = Math.pow(lambda, this.rank) * _k_distance
  override def lrd = Math.pow(lambda, this.rank) * _lrd

}