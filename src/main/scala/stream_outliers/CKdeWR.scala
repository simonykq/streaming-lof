package stream_outliers

import breeze.linalg._
import breeze.stats._
import org.apache.flink.api.common.state.{ListStateDescriptor, ListState}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionSnapshotContext, FunctionInitializationContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

class CKdeWR extends AllWindowFunction[Dataset, ResultSummary[Dataset], GlobalWindow] with CheckpointedFunction {

  @transient
  private var binnedSummaryState: ListState[(Long, Bin)] = _
  @transient
  private var candidateOutlierState: ListState[Dataset] = _

  private val binnedSummary = HashMap[Long, Bin]()
  private val candidateOutliers = ListBuffer[Dataset]()
//  private var averageDensity = 0.0
//  private var totalCount = 0

  private var threshold = 0.0

  private var bounds: DenseMatrix[Double] = null

  private final val k = 100
  private final val xi = 0.5
  private final val rank = 5
  private final val lambda = 0.5

  override def apply(window: GlobalWindow, windowData: Iterable[Dataset], out: Collector[ResultSummary[Dataset]]): Unit = {

    val bandwidths = _get_bandwidths(windowData)
    val pointsWithDensities = _cumulative_density(windowData ++ candidateOutliers, windowData, binnedSummary, bandwidths)

    if (bounds == null)
      bounds = DenseMatrix.zeros[Double](bandwidths.length,2)

    _update_threshold(DenseVector[Double](pointsWithDensities.map(_._2).toArray))
    _update_ranks(pointsWithDensities)

    out.collect(_get_result(pointsWithDensities))

//    println("Size:" + windowData.size)
//    println("Bandwidth:" + bandwidths)
//    println("Average density:" + averageDensity)
//    println("Candidates:" + candidateOutliers.size)
//    println(s"Bin summary: ${binnedSummary.keys} (${binnedSummary.size})")
//    println("Bounds:" + bounds)
//    println(s"Threshold: ${threshold}")

    _update_candidate_outliers(windowData)
    _update_bounds(windowData)
    _update_bins(windowData)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

    val descriptor = new ListStateDescriptor[(Long, Bin)](
      "binned-summary",
      TypeInformation.of(new TypeHint[(Long, Bin)]() {})
    )

    val descriptor_2 = new ListStateDescriptor[Dataset](
      "candidate-outliers",
      TypeInformation.of(new TypeHint[Dataset]() {})
    )

    binnedSummaryState = context.getOperatorStateStore.getUnionListState(descriptor)
    candidateOutlierState = context.getOperatorStateStore.getUnionListState(descriptor_2)

    if(context.isRestored) {
      val iterator = binnedSummaryState.get().iterator()
      while(iterator.hasNext) {
        val value = iterator.next()
        binnedSummary.put(value._1, value._2)
      }
      val iterator2 = candidateOutlierState.get().iterator()
      while(iterator2.hasNext) {
        candidateOutliers += iterator2.next()
      }

    }

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {

    binnedSummaryState.clear()
    candidateOutlierState.clear()
    for((key: Long, value: Bin) <- binnedSummary) {
      binnedSummaryState.add((key, value))
    }
    for(value <- candidateOutliers) {
      candidateOutlierState.add(value)
    }

  }

  def _get_bandwidths(windowData: Iterable[Dataset]): DenseVector[Double] = {
    val matrix = windowData.par.map(_.attrs.toDenseMatrix).reduce(DenseMatrix.vertcat(_,_))
    val s_deviation = stddev(matrix(::, *)).inner
    s_deviation.map(_ * Math.pow(windowData.size, 1.0 / (matrix.cols + 4))).map { b =>
      if (b == 0)
        1.0
      else
        b
    }
  }

  def _update_bounds(windowData: Iterable[Dataset]): Unit = {
    val matrix = windowData.par.map(_.attrs.toDenseMatrix).reduce(DenseMatrix.vertcat(_,_))
    (min(matrix(::, *)).inner.toArray zip max(matrix(::, *)).inner.toArray).zipWithIndex.foreach {
      case ((lower, upper), index) =>
        val (min, max) = (bounds(index, 0), bounds(index, 1))
        if(lower < min || min == 0) bounds(index, 0) = lower
        if(upper > max) bounds(index, 1) = upper
    }

  }

  def _update_bins(windowData: Iterable[Dataset]): Unit = {
    val updatedIndices = windowData.groupBy(_calculate_bin_index).map {
      case (index, list) =>
        val c_ij = list.size
        val accumM = list.map(_.attrs.toDenseMatrix).reduce(DenseMatrix.vertcat(_,_))
        val u_ij = mean(accumM(::, *)).inner

        binnedSummary.get(index) match {
          case Some(bin) =>
            bin.mean = (bin.mean.map(_ * bin.count) + u_ij.map(_ * c_ij)).map(_ / (bin.count + c_ij))
            bin.count += c_ij
            bin.rank = 0
          case None =>
            binnedSummary.put(index, Bin(u_ij, c_ij, 0))
        }
        index
    }

    (binnedSummary.keySet diff updatedIndices.toSet).foreach { index =>

      binnedSummary.get(index) match {
        case Some(bin) =>
          bin.rank += 1
      }

    }
  }

  def _calculate_bin_index(data: Dataset): Long = {

    val (min, max) = (bounds(::, 0), bounds(::, 1))
    val delta = (max - min).map { d =>
      if (d == 0)
        1.0
      else
        d / k
    }
    ((data.attrs - min) /:/ delta).toArray.zipWithIndex.map {
      case (i_j, index) =>
        i_j.toInt * Math.pow(k, index).toLong
    }.sum
  }

  def _update_candidate_outliers(windowData: Iterable[Dataset]): Unit = {

    for(data <- windowData if data.rank > 0)
      candidateOutliers += data

    for(data <- candidateOutliers if data.rank == 0 || data.rank == rank)
      candidateOutliers -= data

  }

  def _get_result(pointWithDensities: Iterable[(Dataset, Double)]): ResultSummary[Dataset] = {
    val (tps, fps) = pointWithDensities.map(_._1).par.filter(_.rank == rank).partition(_.label == 1)
    val (tns, fns) = pointWithDensities.map(_._1).par.filter(_.rank == 0).partition(_.label == 0)
    new ResultSummary(pointWithDensities.map(_._1), tps.size, fps.size, fns.size, tns.size)
  }

  def _update_ranks(pointWithDensities: Iterable[(Dataset, Double)]): Unit =
    pointWithDensities.par partition { p_with_den => p_with_den._2 < threshold  } match {
      case (outliersWithDensities, inliersWithDensities) =>
        for((outlier, _) <- outliersWithDensities) {
          outlier.rank += 1
        }

        for((inlier, _) <- inliersWithDensities) {
          if(inlier.rank > 0)
            inlier.rank -= 1
        }
    }

//  def _update_average_density(densities: DenseVector[Double]): Unit = {
//
//    averageDensity = (averageDensity * totalCount + sum(densities)) / (totalCount + densities.length)
//    totalCount += densities.length
//  }

  def _update_threshold(densities: DenseVector[Double]): Unit = {
//    threshold = mean(densities) - 3 * stddev(densities)
    threshold = mean(densities) * xi
  }

  def _cumulative_density(q_points: Iterable[Dataset],
                          r_points_window: Iterable[Dataset],
                          r_points_bin: Iterable[(Long, Bin)],
                          bandwidth: DenseVector[Double]): Iterable[(Dataset, Double)] =

    if(r_points_bin.isEmpty)
      _density_over_window(q_points, r_points_window, bandwidth)
    else
      _density_over_window(q_points, r_points_window, bandwidth) zip _density_over_bin(q_points, r_points_bin, bandwidth) map
        { case ((point, den_1), (_, den_2)) => (point, den_1 + den_2) }


  def _density_over_window(q_points: Iterable[Dataset],
                           r_points: Iterable[Dataset],
                           bandwidth: DenseVector[Double]): Iterable[(Dataset, Double)] =
    q_points.par.map { q_point =>
      val density = r_points.par.map { r_point =>
        q_point.gaussian_kernel(r_point.attrs, bandwidth)
      }.sum / r_points.size
      (q_point, density)
    }.seq
//    for(q_point <- q_points.par)
//      yield {
//        val density = (for (r_point <- r_points) yield q_point.gaussian_kernel(r_point.attrs, bandwidth)).sum / r_points.size
//        (q_point, density)
//      }


  def _density_over_bin(q_points: Iterable[Dataset],
                        r_points: Iterable[(Long, Bin)],
                        bandwidth: DenseVector[Double]): Iterable[(Dataset, Double)] =
    q_points.par.map { q_point =>
      val density = r_points.par.map { tuple =>
        q_point.gaussian_kernel(tuple._2.mean, bandwidth, Math.pow(lambda, tuple._2.rank) * tuple._2.count)
      }.sum / r_points.map(_._2.count).sum
      (q_point, density)
    }.seq
//    for(q_point <- q_points)
//      yield {
//        val density = (for((index: Long, bin: Bin) <- r_points) yield q_point.gaussian_kernel(bin.mean, bandwidth, Math.pow(lambda, bin.rank) * bin.count)).sum / r_points.map(_._2.count).sum
//        (q_point, density)
//      }

}