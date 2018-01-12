package com.wavesplatform.it.transactions

import com.wavesplatform.it.api.NodeApi
import com.wavesplatform.it.util._

import scala.concurrent.Await
import scala.concurrent.duration._

class PaymentTransactionSuite extends BaseTransactionSuite {

  private val paymentAmount = 5.waves
  private val defaulFee = 1.waves

  test("waves payment changes waves balances and eff.b.") {
    val f = for {
      ((firstBalance, firstEffBalance), (secondBalance, secondEffBalance)) <- accountBalances(firstAddress)
        .zip(accountBalances(secondAddress))

      transferId <- sender.payment(firstAddress, secondAddress, paymentAmount, defaulFee).map(_.id)
      _ <- waitForHeightAraiseAndTxPresent(transferId, 1)
      _ <- assertBalances(firstAddress, firstBalance - paymentAmount - defaulFee, firstEffBalance - paymentAmount - defaulFee)
        .zip(assertBalances(secondAddress, secondBalance + paymentAmount, secondEffBalance + paymentAmount))
    } yield succeed

    Await.result(f, 2.minute)
  }

  test("obsolete endpoints respond with BadRequest") {
    val payment = NodeApi.PaymentRequest(5.waves, 1.waves, firstAddress, secondAddress)
    val errorMessage = "This API is no longer supported"
    val f = for {
      _ <- assertBadRequestAndMessage(sender.postJson("/waves/payment/signature", payment), errorMessage)
      _ <- assertBadRequestAndMessage(sender.postJson("/waves/create-signed-payment", payment), errorMessage)
      _ <- assertBadRequestAndMessage(sender.postJson("/waves/external-payment", payment), errorMessage)
      _ <- assertBadRequestAndMessage(sender.postJson("/waves/broadcast-signed-payment", payment), errorMessage)
    } yield succeed

    Await.result(f, 1.minute)
  }
}
