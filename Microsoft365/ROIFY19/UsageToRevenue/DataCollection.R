options(stringsAsFactors=FALSE, cores=19)

require(dplyr)
require(lubridate)
require(tidyr)
require(data.table)
require(readxl)

#ROI COHORT
roi_cohort <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\ROICohortFY20.csv") #46k TPIDs
mal_tenants <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FY20MALTenants.csv")
colnames(mal_tenants)[1] <- "TenantId"
roi_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\ROI\\SumOfWorkloadsROIOutput.csv")
roi_output <- inner_join(roi_output, mal_tenants, by = "TenantId") #30k TPIDs
roi_output <- roi_output %>% select(TenantId, Tpid, SumMau, SumMauPrev, MAUpred, MAUpred_NoInv, MAUpred_NoFT, MAUpred_NoCSM,
                                    MAUpred_NoECIF, MAUpred_NoPARTNER, MAUpred_NoATS, MAUpred_NoMCS, MAUpred_NoPREMIER)

# SUM MAU
roi_output <- roi_output %>% group_by(Tpid) %>% summarise(SumMauDec2019Actual = sum(SumMau),
                                                          SumMauDec2018Actual = sum(SumMauPrev),
                                                          SumMauDec2019Pred = sum(MAUpred),
                                                          SumMauDec2019NoInv = sum(MAUpred_NoInv),
                                                          SumMauDec2019NoFT = sum(MAUpred_NoFT),
                                                          SumMauDec2019NoCSM = sum(MAUpred_NoCSM),
                                                          SumMauDec2019NoECIF = sum(MAUpred_NoECIF),
                                                          SumMauDec2019NoPARTNER = sum(MAUpred_NoPARTNER),
                                                          SumMauDec2019NoATS = sum(MAUpred_NoATS),
                                                          SumMauDec2019NoMCS = sum(MAUpred_NoMCS),
                                                          SumMauDec2019NoPREMIER = sum(MAUpred_NoPREMIER))

dec2017 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\SumOfMAU\\2017-12_AllUpMauByWorkload.csv")
colnames(dec2017) <- c("Date", "OmsTenantId", "Workload", "Mau", "Tpid")
dec2017$Date <- "12/31/2017 12:00:00 AM"
dec2017 <- dec2017 %>% group_by(Tpid, Workload) %>% summarise(Mau = sum(Mau))
dec2017 <- spread(dec2017, Workload, Mau)
dec2017[is.na(dec2017)] <- 0
dec2017$SumMauDec2017Actual <- dec2017$EXO + dec2017$ODB + dec2017$OfficeClient + dec2017$SPO + dec2017$Teams
dec2017 <- dec2017 %>% select(Tpid, SumMauDec2017Actual)

roi_output <- left_join(roi_output, dec2017, by = "Tpid")
roi_output[is.na(roi_output)] <- 0

# CS MAU
csmau_201802 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\CSMAU\\CSMAU_201802.csv")
csmau_201802 <- csmau_201802 %>% group_by(Tpid) %>% summarise(PAU = sum(PaidAvailableUnits), 
                                                              CSMAU = sum(O365MAU), 
                                                              TeamsPAU201802 = sum(TeamsPaidAvailableUnits))

csmau_201802$CsMauUsage201802 <- (csmau_201802$CSMAU/csmau_201802$PAU)
csmau_201802 <- csmau_201802 %>% select(Tpid, CsMauUsage201802, TeamsPAU201802)

csmau_201901 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\CSMAU\\CSMAU_201901.csv")                                                            
csmau_201901 <- csmau_201901 %>% group_by(Tpid) %>% summarise(PAU = sum(PaidAvailableUnits), 
                                                              CSMAU = sum(O365MAU), 
                                                              TeamsPAU201901 = sum(TeamsPaidAvailableUnits))
csmau_201901$CsMauUsage201901 <- (csmau_201901$CSMAU/csmau_201901$PAU)
csmau_201901 <- csmau_201901 %>% select(Tpid, CsMauUsage201901, TeamsPAU201901)

roi_output <- left_join(roi_output, csmau_201802, by = "Tpid")
roi_output <- left_join(roi_output, csmau_201901, by = "Tpid")
roi_output[is.na(roi_output)] <- 0
roi_output$CsMauUsage201802[is.nan(roi_output$CsMauUsage201802)] <- 0
roi_output$CsMauUsage201901[is.nan(roi_output$CsMauUsage201901)] <- 0
roi_output$CsMauUsage201802[is.infinite(roi_output$CsMauUsage201802)] <- 0
roi_output$CsMauUsage201901[is.infinite(roi_output$CsMauUsage201901)] <- 0


# AREA 
area <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Area.csv")  
colnames(area)[1] <- "Tpid"
roi_output <- left_join(roi_output, area, by = "Tpid")

# REVENUE - OnPrem and Online
revenue <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Revenue\\OnPremOnline.xlsx")
colnames(revenue)[1] <- "Tpid"
colnames(revenue)[2] <- "Service"
revenue[is.na(revenue)] <- 0

revenue <- revenue %>% group_by(Tpid, Service) %>% summarise(FY16Q3BilledRevenueQTD = sum(FY16Q3BilledRevenueQTD),
                                                             FY16Q4BilledRevenueQTD = sum(FY16Q4BilledRevenueQTD),
                                                             FY17Q1BilledRevenueQTD = sum(FY17Q1BilledRevenueQTD),
                                                             FY17Q2BilledRevenueQTD = sum(FY17Q2BilledRevenueQTD),
                                                             FY17Q3BilledRevenueQTD = sum(FY17Q3BilledRevenueQTD),
                                                             FY17Q4BilledRevenueQTD = sum(FY17Q4BilledRevenueQTD),
                                                             FY18Q1BilledRevenueQTD = sum(FY18Q1BilledRevenueQTD),
                                                             FY18Q2BilledRevenueQTD = sum(FY18Q2BilledRevenueQTD),
                                                             FY18Q3BilledRevenueQTD = sum(FY18Q3BilledRevenueQTD),
                                                             FY18Q4BilledRevenueQTD = sum(FY18Q4BilledRevenueQTD),
                                                             FY19Q1BilledRevenueQTD = sum(FY19Q1BilledRevenueQTD),
                                                             FY19Q2BilledRevenueQTD = sum(FY19Q2BilledRevenueQTD))
                                                             
#OnPrem variables 
onprem_rev <- filter(revenue, Service == "On-Prem")

onprem_rev$OnPremRevfy18Q3fy19Q2 <- onprem_rev$FY18Q3BilledRevenueQTD +  onprem_rev$FY18Q4BilledRevenueQTD + 
                                    onprem_rev$FY19Q1BilledRevenueQTD + onprem_rev$FY19Q2BilledRevenueQTD

onprem_rev$OnPremRevfy17Q3fy18Q2 <- onprem_rev$FY17Q3BilledRevenueQTD +  onprem_rev$FY17Q4BilledRevenueQTD + 
                                    onprem_rev$FY18Q1BilledRevenueQTD + onprem_rev$FY18Q2BilledRevenueQTD

onprem_rev$OnPremRevfy16Q3fy17Q2 <- onprem_rev$FY16Q3BilledRevenueQTD +  onprem_rev$FY16Q4BilledRevenueQTD + 
                                    onprem_rev$FY17Q1BilledRevenueQTD + onprem_rev$FY17Q2BilledRevenueQTD
onprem_rev <- onprem_rev %>% select(Tpid, OnPremRevfy18Q3fy19Q2, OnPremRevfy17Q3fy18Q2, OnPremRevfy16Q3fy17Q2)
onprem_rev$Tpid <- as.numeric(onprem_rev$Tpid)

# Online Revenue
online_rev <- filter(revenue, Service == "Online")

online_rev$OnlineRevfy18Q3fy19Q2 <- online_rev$FY18Q3BilledRevenueQTD +  online_rev$FY18Q4BilledRevenueQTD + 
                                    online_rev$FY19Q1BilledRevenueQTD + online_rev$FY19Q2BilledRevenueQTD

online_rev$OnlineRevfy17Q3fy18Q2 <- online_rev$FY17Q3BilledRevenueQTD +  online_rev$FY17Q4BilledRevenueQTD + 
                                    online_rev$FY18Q1BilledRevenueQTD + online_rev$FY18Q2BilledRevenueQTD

online_rev$OnlineRevfy16Q3fy17Q2 <- online_rev$FY16Q3BilledRevenueQTD +  online_rev$FY16Q4BilledRevenueQTD + 
                                    online_rev$FY17Q1BilledRevenueQTD + online_rev$FY17Q2BilledRevenueQTD

online_rev <- online_rev %>% select(Tpid, OnlineRevfy18Q3fy19Q2, OnlineRevfy17Q3fy18Q2, OnlineRevfy16Q3fy17Q2)
online_rev$Tpid <- as.numeric(online_rev$Tpid)

roi_output <- left_join(roi_output, onprem_rev, by = "Tpid")
roi_output <- left_join(roi_output, online_rev, by = "Tpid")

roi_output[is.na(roi_output)] <- 0

# WIN 10
win10_mad <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Win10\\Win10MAD.xlsx")
colnames(win10_mad)[1] <- "Tpid"
win10_mad <- win10_mad %>% select(Tpid, `31012019_MAD`,`31012018_MAD`)

roi_output <- left_join(roi_output, win10_mad, by = "Tpid")
roi_output[is.na(roi_output)] <- 0

#DISCOUNTING
fy18q1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY18Q1")
fy18q2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY18Q2")
fy18q3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY18Q3")
fy18q4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY18Q4")
fy19q1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY19Q1")
fy19q2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY19Q2")
fy19q3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY19Q3")
fy19q4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY19Q4")
fy20q1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY20Q1")
fy20q2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Discounting\\OnPremDiscounting.xlsx", sheet = "FY20Q2")

discounting <- full_join(fy18q3, fy18q4, by = "Tpid") %>% 
                full_join(., fy19q1, by = "Tpid") %>% 
                full_join(., fy19q2, by = "Tpid") %>% 
                full_join(., fy19q3, by = "Tpid") %>% 
                full_join(., fy19q4, by = "Tpid") %>% 
                full_join(., fy20q1, by = "Tpid") %>% 
                full_join(., fy20q2, by = "Tpid") 
  
discounting[is.na(discounting)] <- 0

discounting$TotalOnPremRevfy18q3fy19q2 <- discounting$OnPremRevFY18Q3 + discounting$OnPremRevFY18Q4 + discounting$OnPremRevFY19Q1 + discounting$OnPremRevFY19Q2 
discounting$TotalOnPremDiscountfy18q3fy19q2 <- discounting$DiscountAmountFY18Q3 + discounting$DiscountAmountFY18Q4 + discounting$DiscountAmountFY19Q1 + discounting$DiscountAmountFY19Q2

discounting$TotalOnPremRevfy19q3fy20q2 <- discounting$OnPremRevFY19Q3 + discounting$OnPremRevFY19Q4 + discounting$OnPremRevFY20Q1 + discounting$OnPremRevFY20Q2 
discounting$TotalOnPremDiscountfy19q3fy20q2 <- discounting$DiscountAmountFY19Q3 + discounting$DiscountAmountFY19Q4 + discounting$DiscountAmountFY20Q1 + discounting$DiscountAmountFY20Q2

discounting <- discounting %>% group_by(Tpid) %>% summarise(OnPremDiscountfy18q3fy19q2 = sum(TotalOnPremDiscountfy18q3fy19q2)/sum(TotalOnPremRevfy18q3fy19q2),
                                                            OnPremDiscountfy19q3fy20q2 = sum(TotalOnPremDiscountfy19q3fy20q2)/sum(TotalOnPremRevfy19q3fy20q2))

discounting$Tpid <- as.numeric(discounting$Tpid)
discounting$OnPremDiscountfy18q3fy19q2[is.nan(discounting$OnPremDiscountfy18q3fy19q2)] <- 0
discounting$OnPremDiscountfy19q3fy20q2[is.nan(discounting$OnPremDiscountfy19q3fy20q2)] <- 0

roi_output <- left_join(roi_output, discounting, by = "Tpid")
roi_output[is.na(roi_output)] <- 0

# write out the final data frame 
write.csv(roi_output, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\U2RData.csv", row.names = FALSE)
