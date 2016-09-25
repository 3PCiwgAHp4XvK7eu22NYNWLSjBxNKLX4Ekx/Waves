package scorex.transaction

object ValidationResult extends Enumeration {
  type ValidationResult = Value

  val ValidateOke = Value(1)
  val InvalidAddress = Value(2)
  val NegativeAmount = Value(3)
  val InsufficientFee = Value(4)
  val NoBalance = Value(5)
}