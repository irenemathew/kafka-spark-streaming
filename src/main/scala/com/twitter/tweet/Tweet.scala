package com.twitter.tweet

object Tweet {

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait TweetModel extends Serializable

  case class TweetData(
                             sentiment: String,
                             text: String,
                             user: String
                             ) extends TweetModel

  object TweetData {
    def apply(array: Array[String]): TweetData = {
      TweetData(
        sentiment = array(0),
        text = array(1),
        user = array(2) )
    }
  }

}
