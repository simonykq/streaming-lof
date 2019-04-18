package stream_outliers

import breeze.stats.distributions.Gaussian
import breeze.linalg.DenseVector


/**
  * Created by simonyu on 2018-06-09.
  */

object Dataset {

  def apply(str: String, kind: String): Dataset = kind match {
      case "KddCup99" =>
        new Dataset(
          DenseVector[Double](_parse_attributes(str.split(",").take(11)).toArray),
          _parse_label(kind, str.split(",").last)
        )
      case "CovType" =>
        new Dataset(
          DenseVector[Double](_parse_attributes(str.split(",").take(10)).toArray),
          _parse_label(kind, str.split(",").last)
        )
      case "Syntactic" =>
        new Dataset(
          DenseVector[Double](_parse_attributes(str.split(",").take(2)).toArray),
          _parse_label(kind, str.split(",").last)
        )
  }



  def _parse_label(kind: String, label: String): Int = kind match {
    case "KddCup99" =>
      label match {
        case "normal." => 0
        case "smurf." => 0
        case "neptune." => 0
        case _ => 1
      }
    case "CovType" =>
      label match {
        case "4" => 1
        case _ => 0
      }
    case "Syntactic" =>
      label match {
        case "1" => 1
        case _ => 0
      }
  }

  def _parse_attributes(attrs: Iterable[String]): Iterable[Double] = attrs.map { w =>
    //      try {
    //        w.toDouble
    //      } catch {
    //        case ex: java.lang.NumberFormatException => w.hashCode().toDouble
    //      }
    if(isAllDigits(w))
      w.toDouble
    else
      w.hashCode().toDouble
  }

  private def isAllDigits(x: String) = x forall Character.isDigit

}

case class Dataset(val attrs: DenseVector[Double], val label: Int, var rank: Int = 0) {

  def gaussian_kernel(attrs: DenseVector[Double], bandwidth: DenseVector[Double], weight: Double = 1): Double = {

    (this.attrs.toArray zip attrs.toArray zip bandwidth.toArray).map {
      case ((x_q, x_r), b) =>
        val gaussian = new Gaussian(x_r, b)
        weight * gaussian.pdf(x_q)
    }.product


//    weight / Math.pow(2 * Math.PI, bandwidth.length / 2.0) * (this.attrs zip attrs zip bandwidth map {
//        case ((x_q, x_r), b) => Math.exp(-1 / 2.0 * Math.pow(x_q / b - x_r / b, 2))
//      }).product
  }

}

case class Bin(var mean: DenseVector[Double], var count: Int, var rank: Int = 0)
