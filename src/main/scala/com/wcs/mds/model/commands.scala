package com.wcs.mds.model

//DataLoader
case object LoadNewData
case object LoadAllData

//SapConnector
case class RequestSapData(beginDate: String, endDate: String, readFlag: String)

//DbReader
case object GetRawTabZhrMds001
case object GetRawTabZhrMds002
case object GetRawTabZhrMds003

