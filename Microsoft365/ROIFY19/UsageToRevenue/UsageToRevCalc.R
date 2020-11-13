options(stringsAsFactors=FALSE, cores=19)

require(dplyr)
require(lubridate)
require(tidyr)
require(data.table)
require(readxl)

roi_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\U2RData.csv")

# $ per subsegment
subsegment_factor <- data.frame(Subsegment = c("2", "31", "57", "56", "26", "246","60", "rem1", "rem2"),
                                Factor = c(34.1, 18.2, 9.5, 9.2, 8.6, 6.5, 5.2, 6.3, 2.3))

############### CY18 Usage to Revenue Calculation ##################

# Map the TPIDS to sub segments #
# TODO - Sum of Dec 2017 MAU - 109.5M (Need to verify)
roi_output_cy18 <- roi_output

roi_output_cy18$Opp <- roi_output_cy18$OnPremRevfy17Q3fy18Q2/(roi_output_cy18$OnPremRevfy17Q3fy18Q2 + roi_output_cy18$OnlineRevfy17Q3fy18Q2)
roi_output_cy18$revIncOP <- (roi_output_cy18$OnPremRevfy17Q3fy18Q2 - roi_output_cy18$OnPremRevfy16Q3fy17Q2)/roi_output_cy18$OnPremRevfy16Q3fy17Q2
roi_output_cy18$Opp[is.nan(roi_output_cy18$Opp)] <- 0
roi_output_cy18$revIncOP[is.nan(roi_output_cy18$revIncOP)] <- 0
roi_output_cy18$revIncOP[is.infinite(roi_output_cy18$revIncOP)] <- 0
roi_output_cy18$logOnPremRevfy17Q3fy18Q2 <- log(roi_output_cy18$OnPremRevfy17Q3fy18Q2)
roi_output_cy18$logOnPremRevfy17Q3fy18Q2[is.infinite(roi_output_cy18$logOnPremRevfy17Q3fy18Q2)] <- 0
roi_output_cy18$logOnPremRevfy17Q3fy18Q2[is.nan(roi_output_cy18$logOnPremRevfy17Q3fy18Q2)] <- 0

roi_output_cy18$Subsegment <-  case_when(roi_output_cy18$logOnPremRevfy17Q3fy18Q2 >= 13.59 ~ "2",
                                       
                                       roi_output_cy18$logOnPremRevfy17Q3fy18Q2 < 13.59 & roi_output_cy18$CsMauUsage201802 < 0.1758 & 
                                         roi_output_cy18$Opp < 0.4656 & roi_output_cy18$OnlineRevfy16Q3fy17Q2 > 11290 ~ "26", 
                                       
                                       roi_output_cy18$logOnPremRevfy17Q3fy18Q2 < 13.59 & roi_output_cy18$CsMauUsage201802 >= 0.1758 & 
                                         roi_output_cy18$Opp >= 0.3076 & roi_output_cy18$OnPremDiscountfy18q3fy19q2 < 0.00075 & roi_output_cy18$CsMauUsage201802 < 0.626 ~ "56", 
                                       
                                       roi_output_cy18$logOnPremRevfy17Q3fy18Q2 < 13.59 & roi_output_cy18$CsMauUsage201802 >= 0.1758 & 
                                         roi_output_cy18$Opp >= 0.3076 & roi_output_cy18$OnPremDiscountfy18q3fy19q2 < 0.00075 & roi_output_cy18$CsMauUsage201802 >= 0.626 ~ "57", 
                                       
                                       roi_output_cy18$logOnPremRevfy17Q3fy18Q2 < 13.59 & roi_output_cy18$CsMauUsage201802 >= 0.1758 & 
                                         roi_output_cy18$Opp < 0.3076 & roi_output_cy18$TeamsPAU201802 >= 116.5 & roi_output_cy18$X31012018_MAD >= 392.5 ~ "60",
                                       
                                       roi_output_cy18$logOnPremRevfy17Q3fy18Q2 < 13.59 & roi_output_cy18$CsMauUsage201802 >= 0.1758 & 
                                         roi_output_cy18$Opp < 0.3076 & roi_output_cy18$TeamsPAU201802 >= 116.5 & roi_output_cy18$X31012018_MAD < 392.5 &
                                         roi_output_cy18$revIncOP < -0.1323 & roi_output_cy18$AreaName == "United States" ~ "246",
                                       
                                       roi_output_cy18$logOnPremRevfy17Q3fy18Q2 < 13.59 & roi_output_cy18$CsMauUsage201802 >= 0.1758 & 
                                         roi_output_cy18$Opp < 0.3076 & roi_output_cy18$TeamsPAU201802 < 116.5 ~ "31",
                                       
                                       roi_output_cy18$OnlineRevfy17Q3fy18Q2 > 0 & roi_output_cy18$OnlineRevfy17Q3fy18Q2 < 1000 ~ "rem1",
                                       
                                       TRUE ~ "rem2")

#Count of accounts per segment
a <- roi_output_cy18 %>% group_by(Subsegment) %>% summarise(count = n())
View(a)

#Scaling to Actual Revenue - V1 - Keep this - Verify final numbers with Ming. (Using Min's file for Online Revenue)
# o365_rev1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Revenue\\CY19Rev.xlsx", sheet = "Sheet2")
# o365_rev1[is.na(o365_rev1)] <- 0
# o365_rev1$CY19Rev <- o365_rev1$FY19Q3BilledRevenueQTD + o365_rev1$FY19Q4BilledRevenueQTD + o365_rev1$FY20Q1BilledRevenueQTD + o365_rev1$FY20Q2BilledRevenueQTD
# colnames(o365_rev1)[1] <- "Tpid"
# o365_rev1 <- o365_rev1 %>% select(Tpid, CY19Rev)
# o365_rev1$Tpid <- as.numeric(o365_rev1$Tpid)
# roi_output_cy181 <- inner_join(roi_output_cy18, o365_rev1, by = "Tpid") #27212 TPIDs Verify
# 
# o365_rev <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Revenue\\OnPremOnline.xlsx")
# o365_rev <- o365_rev %>% filter(`Is Online Service` == "Online")
# o365_rev[is.na(o365_rev)] <- 0
# o365_rev$CY18Rev <- o365_rev$FY18Q3BilledRevenueQTD + o365_rev$FY18Q4BilledRevenueQTD + o365_rev$FY19Q1BilledRevenueQTD + o365_rev$FY19Q2BilledRevenueQTD
# o365_rev <- o365_rev %>% group_by(TPID) %>% summarise(CY18Rev = sum(CY18Rev))
# colnames(o365_rev)[1] <- "Tpid"
# o365_rev <- o365_rev %>% select(Tpid, CY18Rev)
# o365_rev$Tpid <- as.numeric(o365_rev$Tpid)
# roi_output_cy182 <- inner_join(roi_output_cy18, o365_rev, by = "Tpid") #28393 TPIDs Verify

# CY18Rev - 12923486560, CY19Rev - 16530294595

#Scaling to Actual Revenue - V2 (using Ming's file for O365 Cloud Rev) - Using this for now. 
o365_rev1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\Revenue\\O365RevenueCY19_Corrected.xlsx")
o365_rev1[is.na(o365_rev1)] <- 0
o365_rev1$CY19Rev <- o365_rev1$FY19Q3BilledRevenueQTD + o365_rev1$FY19Q4BilledRevenueQTD + o365_rev1$FY20Q1BilledRevenueQTD + o365_rev1$FY20Q2BilledRevenueQTD
o365_rev1$CY18Rev <- o365_rev1$FY18Q3BilledRevenueQTD + o365_rev1$FY18Q4BilledRevenueQTD + o365_rev1$FY19Q1BilledRevenueQTD + o365_rev1$FY19Q2BilledRevenueQTD
colnames(o365_rev1)[1] <- "Tpid"
o365_rev1 <- o365_rev1 %>% select(Tpid, CY18Rev, CY19Rev)
o365_rev1$Tpid <- as.numeric(o365_rev1$Tpid)
roi_output_cy18 <- inner_join(roi_output_cy18, o365_rev1, by = "Tpid") #29336 TPIDs - The ones with large MAU seem to be deleted accounts. 

#### Putting together all data ####
# > sum(roi_output_cy18$CY19Rev)
# [1] 16524685430
# > sum(roi_output_cy18$CY18Rev)
# [1] 12987355791
# > 16524685430 - 12987355791
# [1] 3537329639

roi_output_cy18 <- inner_join(roi_output_cy18, subsegment_factor, by = "Subsegment")                                       

rev_output <- roi_output_cy18 %>% group_by(Subsegment) %>% summarise(ActualMAUGrowth1718 = sum(SumMauDec2018Actual) - sum(SumMauDec2017Actual), 
                                                                     Factor = max(Factor))
rev_output$AttrRevPerSegment <- rev_output$ActualMAUGrowth1718 * rev_output$Factor

# > sum(rev_output$ActualMAUGrowth1718)
# [1] 65504699
# > sum(rev_output$AttrRevPerSegment) 
# [1] 1097566960
      
totalrevattrInv <- sum(rev_output$AttrRevPerSegment) #ESTIMATED REVENUE GROWTH DRIVEN BY USAGE GROWTH FROM SEGMENTS in CY19 - 1097566960    

actRevGrowth <- sum(roi_output_cy18$CY19Rev) - sum(roi_output_cy18$CY18Rev)
actRevGrowth <- 0.25 * actRevGrowth  # ACTUAL REVENUE GROWTH DRIVEN BY USAGE IN CY19 - 884332410
scalingFactor <- actRevGrowth/totalrevattrInv #0.80

#Attribution per investment
overall <- 0.49 * totalrevattrInv * scalingFactor #433322881
ftattr <- 0.76 * 0.49 * totalrevattrInv * scalingFactor # 329325389
csmattr <- 0.15 * 0.49 * totalrevattrInv * scalingFactor # 64998432
ecifattr <- 0.01 * 0.49 * totalrevattrInv * scalingFactor # 4333229
partnerattr <- 0.02 * 0.49 * totalrevattrInv * scalingFactor # 8666458
atsattr <- 0.06 * 0.49 * totalrevattrInv * scalingFactor # 25999373

############### CY19 Usage to Future Revenue Calculation ##################

roi_output_cy19 <- roi_output

roi_output_cy19$Opp <- roi_output_cy19$OnPremRevfy18Q3fy19Q2/(roi_output_cy19$OnPremRevfy18Q3fy19Q2 + roi_output_cy19$OnlineRevfy18Q3fy19Q2)
roi_output_cy19$revIncOP <- (roi_output_cy19$OnPremRevfy18Q3fy19Q2 - roi_output_cy19$OnPremRevfy17Q3fy18Q2)/roi_output_cy19$OnPremRevfy17Q3fy18Q2
roi_output_cy19$Opp[is.nan(roi_output_cy19$Opp)] <- 0
roi_output_cy19$revIncOP[is.nan(roi_output_cy19$revIncOP)] <- 0
roi_output_cy19$revIncOP[is.infinite(roi_output_cy19$revIncOP)] <- 0
roi_output_cy19$logOnPremRevfy18Q3fy19Q2 <- log(roi_output_cy19$OnPremRevfy18Q3fy19Q2)
roi_output_cy19$logOnPremRevfy18Q3fy19Q2[is.infinite(roi_output_cy19$logOnPremRevfy18Q3fy19Q2)] <- 0
roi_output_cy19$logOnPremRevfy18Q3fy19Q2[is.nan(roi_output_cy19$logOnPremRevfy18Q3fy19Q2)] <- 0

roi_output_cy19$Subsegment <-  case_when(roi_output_cy19$logOnPremRevfy18Q3fy19Q2 >= 13.59 ~ "2",
                                    
                                    roi_output_cy19$logOnPremRevfy18Q3fy19Q2 < 13.59 & roi_output_cy19$CsMauUsage201901 < 0.1758 & 
                                      roi_output_cy19$Opp < 0.4656 & roi_output_cy19$OnlineRevfy17Q3fy18Q2 > 11290 ~ "26", 
                                    
                                    roi_output_cy19$logOnPremRevfy18Q3fy19Q2 < 13.59 & roi_output_cy19$CsMauUsage201901 >= 0.1758 & 
                                      roi_output_cy19$Opp >= 0.3076 & roi_output_cy19$OnPremDiscountfy19q3fy20q2 < 0.00075 & roi_output_cy19$CsMauUsage201901 < 0.626 ~ "56", 
                                    
                                    roi_output_cy19$logOnPremRevfy18Q3fy19Q2 < 13.59 & roi_output_cy19$CsMauUsage201901 >= 0.1758 & 
                                      roi_output_cy19$Opp >= 0.3076 & roi_output_cy19$OnPremDiscountfy19q3fy20q2 < 0.00075 & roi_output_cy19$CsMauUsage201901 >= 0.626 ~ "57", 
                                    
                                    roi_output_cy19$logOnPremRevfy18Q3fy19Q2 < 13.59 & roi_output_cy19$CsMauUsage201901 >= 0.1758 & 
                                      roi_output_cy19$Opp < 0.3076 & roi_output_cy19$TeamsPAU201901 >= 116.5 & roi_output_cy19$X31012019_MAD >= 392.5 ~ "60",
                                    
                                    roi_output_cy19$logOnPremRevfy18Q3fy19Q2 < 13.59 & roi_output_cy19$CsMauUsage201901 >= 0.1758 & 
                                      roi_output_cy19$Opp < 0.3076 & roi_output_cy19$TeamsPAU201901 >= 116.5 & roi_output_cy19$X31012019_MAD < 392.5 &
                                      roi_output_cy19$revIncOP < -0.1323 & roi_output_cy19$AreaName == "United States" ~ "246",
                                    
                                    roi_output_cy19$logOnPremRevfy18Q3fy19Q2 < 13.59 & roi_output_cy19$CsMauUsage201901 >= 0.1758 & 
                                      roi_output_cy19$Opp < 0.3076 & roi_output_cy19$TeamsPAU201901 < 116.5 ~ "31",
                                    
                                    roi_output_cy19$OnlineRevfy18Q3fy19Q2 > 0 & roi_output_cy19$OnlineRevfy18Q3fy19Q2 < 1000 ~ "rem1",
                                    
                                    TRUE ~ "rem2")

#Count of accounts per segment
a <- roi_output_cy19 %>% group_by(Subsegment) %>% summarise(count = n())
View(a)

roi_output_cy19 <- inner_join(roi_output_cy19, subsegment_factor, by = "Subsegment") 
roi_output_cy18_tpids <- roi_output_cy18 %>% select(Tpid)
roi_output_cy19 <- inner_join(roi_output_cy19, roi_output_cy18_tpids, by = "Tpid")


roi_output_cy19$ActualMAUGrowth1819 <- roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2018Actual #Actual MAu growth CY19 - 86198325
roi_output_cy19$PredictedMAUGrowth1819 <- roi_output_cy19$SumMauDec2019Pred - roi_output_cy19$SumMauDec2018Actual #Predicted MAu growth CY19 - 82202354

#scaling all predicted values 
roi_output_cy19$SumMauDec2019NoInvScaled <- (roi_output_cy19$SumMauDec2019NoInv * roi_output_cy19$SumMauDec2019Actual)/roi_output_cy19$SumMauDec2019Pred
roi_output_cy19$SumMauDec2019NoFTScaled <- (roi_output_cy19$SumMauDec2019NoFT * roi_output_cy19$SumMauDec2019Actual)/roi_output_cy19$SumMauDec2019Pred
roi_output_cy19$SumMauDec2019NoCSMScaled <- (roi_output_cy19$SumMauDec2019NoCSM * roi_output_cy19$SumMauDec2019Actual)/roi_output_cy19$SumMauDec2019Pred
roi_output_cy19$SumMauDec2019NoECIFScaled <- (roi_output_cy19$SumMauDec2019NoECIF * roi_output_cy19$SumMauDec2019Actual)/roi_output_cy19$SumMauDec2019Pred
roi_output_cy19$SumMauDec2019NoPARTNERScaled <- (roi_output_cy19$SumMauDec2019NoPARTNER * roi_output_cy19$SumMauDec2019Actual)/roi_output_cy19$SumMauDec2019Pred
roi_output_cy19$SumMauDec2019NoATSScaled <- (roi_output_cy19$SumMauDec2019NoATS * roi_output_cy19$SumMauDec2019Actual)/roi_output_cy19$SumMauDec2019Pred

#Investment wise attributable to investments driving usage
roi_output_cy19$TotalUsageRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2018Actual) * roi_output_cy19$Factor 

#Aggregate level 
sum(roi_output_cy19$TotalUsageRev) #1250381096 - Actual Usage driven revenue
sum(roi_output_cy19$TotalUsageRev) * 0.49
0.76 * 0.49 * sum(roi_output_cy19$TotalUsageRev)
0.15 * 0.49 * sum(roi_output_cy19$TotalUsageRev)
0.01 * 0.49 * sum(roi_output_cy19$TotalUsageRev) 
0.02 * 0.49 * sum(roi_output_cy19$TotalUsageRev) 
0.06 * 0.49 * sum(roi_output_cy19$TotalUsageRev) 

#Account level
roi_output_cy19$InvAttrRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2019NoInvScaled) * roi_output_cy19$Factor #782593710
roi_output_cy19$FTAttrRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2019NoFTScaled) * roi_output_cy19$Factor
roi_output_cy19$CSMAttrRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2019NoCSMScaled) * roi_output_cy19$Factor
roi_output_cy19$ECIFAttrRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2019NoECIFScaled) * roi_output_cy19$Factor
roi_output_cy19$PartnerAttrRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2019NoPARTNERScaled) * roi_output_cy19$Factor
roi_output_cy19$ATSAttrRev <- (roi_output_cy19$SumMauDec2019Actual - roi_output_cy19$SumMauDec2019NoATSScaled) * roi_output_cy19$Factor

invattr <- sum(roi_output_cy19$InvAttrRev)
ftattr <- sum(roi_output_cy19$FTAttrRev)
csmattr <- sum(roi_output_cy19$CSMAttrRev)
ecifattr <- sum(roi_output_cy19$ECIFAttrRev)
partnerattr <- sum(roi_output_cy19$PartnerAttrRev)
atsattr <- sum(roi_output_cy19$ATSAttrRev)
totalattr <- ftattr + csmattr + ecifattr + partnerattr + atsattr
(ftattr/totalattr) * invattr
(csmattr/totalattr) * invattr
(ecifattr/totalattr) * invattr
(partnerattr/totalattr) * invattr
(atsattr/totalattr) * invattr

#write out account level projections - select required columns
roi_output_cy19_csv <- roi_output_cy19 %>% select(Tpid, SumMauDec2019Actual, SumMauDec2018Actual, SumMauDec2019Pred, SumMauDec2019NoInv, SumMauDec2019NoInvScaled, TotalUsageRev, InvAttrRev)
write.csv(roi_output_cy19_csv, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\UsageToRevenue\\CY20RevenueProjectionsPerTPID.csv", row.names = FALSE)
