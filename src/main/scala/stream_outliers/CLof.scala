package stream_outliers

import breeze.linalg._
import breeze.stats._

import scala.collection.mutable.HashMap

import org.apache.flink.api.common.state.{ListStateDescriptor, ListState}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * Created by simonyu on 2018-07-08.
  */
class CLof extends AllWindowFunction[TPoint, Result[TPoint], GlobalWindow] with CheckpointedFunction {

  @transient
  private var clusterState: ListState[(Long, VPoint)] = _
  private val clusters = HashMap[Long, VPoint]()

  private var bounds: DenseMatrix[Double] = null

  private final val k = 10
  private final val lambda = 0.5

  private var threshold = 0.0


  override def apply(window: GlobalWindow, windowPoints: Iterable[TPoint], out: Collector[Result[TPoint]]): Unit = {

    //    if (windowPoints.size <= StreamingJob.WINDOW_SIZE - 1)
    // do clustering

    if (windowPoints.size == 1)
      bounds = DenseMatrix.zeros[Double](windowPoints.iterator.next().value.length, 2)

    if (windowPoints.size == StreamingJob.WINDOW_SIZE) {

      //increase ranks of every cluster by 1
      for (cluster <- clusters.values)
        cluster.rank += 1

      val referencePoints = windowPoints ++ clusters.values
      val pointWithLofs = _cumulative_lof(windowPoints, referencePoints)
      _update_threshold(DenseVector[Double](pointWithLofs.map(_._2).toArray))

      val (pointToExpire, lof) = pointWithLofs.toSeq(pointWithLofs.size - 1)

      out.collect(_get_result(pointToExpire, lof))

      _update_bounds(windowPoints)
      _update_clusters(pointToExpire)

    }

  }

  def _get_result(pointToExpire: TPoint, lof: Double): Result[TPoint] = {

    if (lof > threshold) {
      if (pointToExpire.label == 1)
        TruePositive(pointToExpire)
      else
        FalsePositive(pointToExpire)
    }
    else {
      if (pointToExpire.label == 1)
        FalseNegative(pointToExpire)
      else
        TrueNegative(pointToExpire)
    }
  }


  def _cumulative_lof(queryPoints: Iterable[TPoint], referencePoints: Iterable[Point]): Iterable[(TPoint, Double)] = {
    for (point <- queryPoints.par)
      point.calculate_k_distance(referencePoints, k)
    for (point <- queryPoints.par)
      point.calculate_lrd(referencePoints, k)
    queryPoints.par.map { point =>
      (point, point.calculate_lof(referencePoints, k))
    }.seq
  }

  def _update_threshold(lofs: DenseVector[Double]): Unit = {
    this.threshold = mean(lofs) + 3 * stddev(lofs)
  }

  def _calculate_bin_index(point: TPoint): Long = {
    val (min, max) = (bounds(::, 0), bounds(::, 1))
    val delta = (max - min).map { d =>
      if (d == 0)
        1.0
      else
        d / k
    }
    ((point.value - min) /:/ delta).toArray.zipWithIndex.map {
      case (i_j, index) =>
        i_j.toInt * Math.pow(k, index).toLong
    }.sum
  }

  def _update_bounds(windowData: Iterable[TPoint]): Unit = {
    val matrix = windowData.par.map(_.value.toDenseMatrix).reduce(DenseMatrix.vertcat(_,_))
    (min(matrix(::, *)).inner.toArray zip max(matrix(::, *)).inner.toArray).zipWithIndex.foreach {
      case ((lower, upper), index) =>
        val (min, max) = (bounds(index, 0), bounds(index, 1))
        if(lower < min || min == 0) bounds(index, 0) = lower
        if(upper > max) bounds(index, 1) = upper
    }
  }

  def _update_clusters(point: TPoint): Unit = {
     val index = _calculate_bin_index(point)
     clusters.get(index) match {
       case Some(vpoint) =>
         vpoint merge point
         vpoint.rank = 0
       case None =>
         val cluster = new VPoint(point.value, lambda)
         cluster.k_distance = point.k_distance
         cluster.lrd = point.lrd
         clusters.put(index, cluster)
     }
  }


  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[(Long, VPoint)](
      "virtual-points",
      TypeInformation.of(new TypeHint[(Long, VPoint)]() {})
    )
    clusterState = context.getOperatorStateStore.getUnionListState(descriptor)

    if (context.isRestored) {
      val iterator = clusterState.get().iterator()
      while (iterator.hasNext) {
        val value = iterator.next()
        clusters.put(value._1, value._2)
      }
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    clusterState.clear()
    for ((key: Long, value: VPoint) <- clusters) {
      clusterState.add((key, value))
    }
  }
}
//         (for {
//           id <- pointToExpire.cluster_id
//           cluster <- clusters.get(id)
//         } yield {
//           //if there exist a cluster that the expired data point belongs
//           cluster merge pointToExpire
//           cluster.rank = 0
//           for(neighbor <- pointToExpire.k_nn(windowPoints, k) if neighbor._1.cluster_id.isEmpty)
//             neighbor._1.cluster_id = Some(id)
//         }) getOrElse {
//           //otherwise form a new cluster, starting from this expired data point
//           val id = randomUUID().toString.toLong
//           val cluster = new VPoint(pointToExpire.value, lambda)
//           cluster.k_distance = pointToExpire.k_distance
//           cluster.lrd = pointToExpire.lrd
//           cluster.rank = 0 //optional
//           clusters.put(id, cluster)
//           for(neighbor <- pointToExpire.k_nn(windowPoints, k) if neighbor._1.cluster_id.isEmpty)
//             neighbor._1.cluster_id = Some(id)
//         }