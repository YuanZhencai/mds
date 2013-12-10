package com.wcs.mds.model

//DataLoader
case object LoadNewData
case object LoadAllData

//SapConnector
case class RequestSapData(beginDate: String, endDate: String, readFlag: String)

