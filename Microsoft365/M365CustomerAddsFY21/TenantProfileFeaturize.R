library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)

smctpids <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smctpids)[1] <- "FinalTPID"
smctpids <- filter(smctpids, FY21.Subsegment %in% c("SM&C Government - Corporate", "Enterprise Growth", "SM&C Commercial - Corporate") ) #27980 Tpids

profile_training <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\TenantProfile\\2019-12_TenantProfile.csv")
profile_training$ym <- "201912"
profile_scoring <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\TenantProfile\\2020-06_TenantProfile.csv") ##REPLACE
profile_scoring$ym <- "202006"

elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

profile_training$FirstPaidStartDate <- as.Date(profile_training$FirstPaidStartDate, "%m/%d/%Y %H:%M:%S")
profile_training$Tenure <- elapsed_months(as.Date("2019-12-31"), profile_training$FirstPaidStartDate)
profile_training$Tenure[is.na(profile_training$Tenure)] <- 1439
write.csv(profile_training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\TenantProfile\\Training_TenantProfile.csv", row.names = FALSE)

profile_scoring$FirstPaidStartDate <- as.Date(profile_scoring$FirstPaidStartDate, "%m/%d/%Y %H:%M:%S")
profile_scoring$Tenure <- elapsed_months(as.Date("2020-06-30"), profile_scoring$FirstPaidStartDate)
profile_scoring$Tenure[is.na(profile_scoring$Tenure)] <- 1439
write.csv(profile_scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\TenantProfile\\Scoring_TenantProfile.csv", row.names = FALSE)
