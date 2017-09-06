package com.wavesplatform.state2.reader

import cats.data.{NonEmptyList => NEL}
import cats.implicits._
import com.wavesplatform.state2._
import monix.eval.Coeval
import scorex.account.{Address, Alias}
import scorex.transaction.Transaction

class CompositeStateReader private(inner: SnapshotStateReader, blockDiff: BlockDiff) extends SnapshotStateReader {

  override def assetDescription(id: ByteStr) = ???

  override def nonZeroLeaseBalances = ???

  override def leaseDetails(leaseId: ByteStr) = ???

  override def leaseInfo(a: Address) = ???

  private val txDiff = blockDiff.txsDiff

  override def transactionInfo(id: ByteStr): Option[(Int, Option[Transaction])] =
    txDiff.transactions.get(id)
      .map(t => (t._1, Some(t._2)))
      .orElse(inner.transactionInfo(id))

  override def accountPortfolio(a: Address): Portfolio =
    inner.accountPortfolio(a).combine(txDiff.portfolios.get(a).orEmpty)

  override def height: Int = inner.height + blockDiff.heightDiff

  override def accountTransactionIds(a: Address, limit: Int): Seq[ByteStr] = {
    val fromDiff = txDiff.accountTransactionIds.get(a).orEmpty
    if (fromDiff.lengthCompare(limit) >= 0) {
      fromDiff.take(limit)
    } else {
      fromDiff ++ inner.accountTransactionIds(a, limit - fromDiff.size) // fresh head ++ stale tail
    }
  }

  override def wavesBalance(a: Address) = {
    val innerBalance = inner.wavesBalance(a)
    txDiff.portfolios.get(a).fold(innerBalance) { p =>
      innerBalance.copy(
        regularBalance = safeSum(innerBalance.regularBalance, p.balance),
        effectiveBalance = safeSum(innerBalance.effectiveBalance, p.leaseInfo.leaseIn) - p.leaseInfo.leaseOut)
    }
  }

  override def assetBalance(a: Address) =
    inner.assetBalance(a) ++ blockDiff.snapshots.get(a).fold(Map.empty[ByteStr, Long])(_.assetBalances)

  override def snapshotAtHeight(acc: Address, h: Int): Option[Snapshot] = ???

  override def paymentTransactionIdByHash(hash: ByteStr): Option[ByteStr]
  = blockDiff.txsDiff.paymentTransactionIdsByHashes.get(hash)
    .orElse(inner.paymentTransactionIdByHash(hash))

  override def aliasesOfAddress(a: Address): Seq[Alias] =
    txDiff.aliases.filter(_._2 == a).keys.toSeq ++ inner.aliasesOfAddress(a)

  override def resolveAlias(a: Alias): Option[Address] = txDiff.aliases.get(a).orElse(inner.resolveAlias(a))

  override def activeLeases: Seq[ByteStr] = {
    blockDiff.txsDiff.leaseState.collect { case (id, isActive) if isActive => id }.toSeq ++ inner.activeLeases
  }

  override def lastUpdateHeight(acc: Address): Option[Int] = ???

  override def containsTransaction(id: ByteStr): Boolean = blockDiff.txsDiff.transactions.contains(id) || inner.containsTransaction(id)

  override def filledVolumeAndFee(orderId: ByteStr): OrderFillInfo =
    blockDiff.txsDiff.orderFills.get(orderId).orEmpty.combine(inner.filledVolumeAndFee(orderId))
}

object CompositeStateReader {
  def composite(blockDiff: BlockDiff, inner: SnapshotStateReader): SnapshotStateReader = new CompositeStateReader(inner, blockDiff)

  def composite(blockDiff: Seq[BlockDiff], inner: SnapshotStateReader): SnapshotStateReader = blockDiff match {
    case (x :: xs) => composite(x, composite(xs, inner))
    case _ => inner
  }

  def composite(blockDiffs: NEL[BlockDiff], inner: SnapshotStateReader): SnapshotStateReader = blockDiffs.tail match {
    case (x :: xs) => composite(blockDiffs.head, composite(NEL(x, xs), inner))
    case Nil => composite(blockDiffs.head, inner)
  }

  // fresh head
  def composite(blockDiff: Coeval[BlockDiff], inner: Coeval[SnapshotStateReader]): Coeval[SnapshotStateReader] = for {
    i <- inner
    b <- blockDiff
  } yield composite(b, i)
}
