package com.mahfooz.spark.dataset.model

case class Flight(
    DEST_COUNTRY_NAME: String,
    ORIGIN_COUNTRY_NAME: String,
    count: BigInt
)
