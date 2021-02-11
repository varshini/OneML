# Data aggregation for usage to revenue #
# Time period used - FY20 H1 is the revenue period. Treatments aand confounders as of the end of FY20 #

library(dplyr)
library(tidyr)
library(readxl)
options(stringsAsFactors = FALSE)

####### Setting the account list and MAL and account attributes #######
mal_fy21 <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\MALFY21.csv") %>% select(TPID, SubSegment) #49876
mal_fy21$TPID <- as.character(mal_fy21$TPID)
acc_attributes <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\AccountAttributes.csv") 
acc_attributes$TPID <- as.character(acc_attributes$TPID)
acc_attributes <- inner_join(mal_fy21, acc_attributes)

####### Economic Factors ###########
# Features - GDP Revenue Average for the previous year (H1, H2), GDP average growth for the previous year
industry_calc_mapping <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\IndustryMappingGrossOutput.csv")
colnames(industry_calc_mapping) <- c("Industry", "GOIndustry")

gross_output_1 <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\GrossOutputRevenue.csv")
colnames(gross_output_1)[1] <- "GOIndustry"
gross_output_1$GDPRevAverageQ1Q2 <- (gross_output_1$X2019Q3 + gross_output_1$X2019Q4)/2
gross_output_1$GDPRevAverageQ3Q4 <- (gross_output_1$X2020Q1 + gross_output_1$X2020Q2)/2
gross_output_1 <- gross_output_1 %>% select(GOIndustry, GDPRevAverageQ1Q2, GDPRevAverageQ3Q4)
gross_output_1 <- inner_join(gross_output_1, industry_calc_mapping)
acc_attributes <- left_join(acc_attributes, gross_output_1) 

gross_output_2 <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\GrossOutputGrowth.csv")
colnames(gross_output_2)[1] <- "GOIndustry"
gross_output_2$GDPGrowthAverageQ1Q2 <- (gross_output_2$X2019Q3 + gross_output_2$X2019Q4)/2
gross_output_2$GDPGrowthAverageQ3Q4 <- (gross_output_2$X2020Q1 + gross_output_2$X2020Q2)/2
gross_output_2 <- gross_output_2 %>% select(GOIndustry, GDPGrowthAverageQ1Q2, GDPGrowthAverageQ3Q4)
gross_output_2 <- inner_join(gross_output_2, industry_calc_mapping)
acc_attributes <- left_join(acc_attributes, gross_output_2)

acc_attributes$Industry <- as.numeric(as.factor(acc_attributes$Industry))
acc_attributes$AreaName <- as.numeric(as.factor(acc_attributes$AreaName))
acc_attributes$SubSegment <- as.numeric(as.factor(acc_attributes$SubSegment))
acc_attributes$SegmentGroup <- as.numeric(as.factor(acc_attributes$SegmentGroup))

# Final account attributes
acc_attributes <- acc_attributes %>% select(TPID, SubSegment, AreaName, Industry, SegmentGroup, GDPRevAverageQ1Q2, GDPRevAverageQ3Q4, 
                                            GDPGrowthAverageQ1Q2, GDPGrowthAverageQ3Q4)

######## Sales activities #########
# Features - Workshops in FY20 Q3 and Q4, ECIF - 
workshop <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\Workshop_Proc.csv")
colnames(workshop)[1] <- "TPID"
workshop[is.na(workshop)] <- 0
workshop$Q3Q4SecurityWorkshops <- workshop$SecurityJan2020 + workshop$SecurityMar2020 + workshop$SecurityMay2020 + workshop$SecurityJun2020
workshop$Q3Q4SecureRemoteWorkWorkshops <- workshop$SecureRemoteWorkMay2020 + workshop$SecureRemoteWorkJun2020
workshop$Q3Q4TeamsAdoptionWorkshops <- workshop$TeamsAdoptionWorkshopMay2020 + workshop$TeamsAdoptionWorkshopJun2020
workshop$Q3Q4ComplianceWorkshops <- workshop$ComplianceWorkshopMar2020 + workshop$ComplianceWorkshopApr2020 + workshop$TeamsAdoptionWorkshopJun2020
workshop$Q3Q4TeamsWorkshops <- workshop$MeetingsWorkshopApr2020 + workshop$TeamsCallingMay2020 + workshop$MeetingsWorkshopJun2020 + workshop$TeamsMeetingsJun2020

workshop <- workshop %>% select(TPID, Q3Q4SecurityWorkshops, Q3Q4SecureRemoteWorkWorkshops, Q3Q4TeamsAdoptionWorkshops, 
                                Q3Q4ComplianceWorkshops, Q3Q4TeamsWorkshops)
workshop$TPID <- as.character(workshop$TPID)

acc_attributes <- left_join(acc_attributes, workshop)
acc_attributes[is.na(acc_attributes)] <- 0



###### PCIB ###########
# Features - Snapshot at the end of June
pcib <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\PCIB.csv")
pcib$AdjustedPCIBDate <- as.Date(pcib$AdjustedPCIBDate, "%m/%d/%Y")
pcib <- pcib %>% filter(AdjustedPCIBDate == as.Date("2020-06-30")) %>% select(TPID, AdjustedPCIB)
pcib <- left_join(acc_attributes, pcib, by = "TPID")

####### Discounting ########
# Features - Avg discount per quarter in the one year before June 2020, Average E5 discount per quarter in the one year
discounting <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\Discounting.csv")
discounting$FirstBilledDateFirstOfTheMonth <- as.Date(discounting$FirstBilledDateFirstOfTheMonth, "%m/%d/%Y")

discounting_e5 <- discounting %>% filter(SuperRevSumDivisionName %in% c("O365 E5 - Non M365", "Windows E5 - M365", "Enterprise Mobility E5 - Non M365", "O365 E5 - M365"
                                                ,"Windows E5 - Non M365"))

discounting_q1_overall <- discounting %>% 
                    filter(FirstBilledDateFirstOfTheMonth >= as.Date("2019-07-01") & 
                                           FirstBilledDateFirstOfTheMonth <= as.Date("2019-09-30") ) %>% 
                    group_by(TPID) %>% summarise(OverallDiscountQ1 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,OverallDiscountQ1)
discounting_q1_e5 <- discounting_e5 %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2019-07-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2019-09-30") ) %>% 
  group_by(TPID) %>% summarise(E5DiscountQ1 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,E5DiscountQ1)

discounting_q2_overall <- discounting %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2019-10-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2019-12-31") ) %>% 
  group_by(TPID) %>% summarise(OverallDiscountQ2 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,OverallDiscountQ2)
discounting_q2_e5 <- discounting_e5 %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2019-10-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2019-12-31") ) %>% 
  group_by(TPID) %>% summarise(E5DiscountQ2 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,E5DiscountQ2)

discounting_q3_overall <- discounting %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2020-01-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2020-03-31") ) %>% 
  group_by(TPID) %>% summarise(OverallDiscountQ3 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,OverallDiscountQ3)
discounting_q3_e5 <- discounting_e5 %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2020-01-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2020-03-31") ) %>% 
  group_by(TPID) %>% summarise(E5DiscountQ3 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,E5DiscountQ3)

discounting_q4_overall <- discounting %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2020-04-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2020-06-30") ) %>% 
  group_by(TPID) %>% summarise(OverallDiscountQ4 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,OverallDiscountQ4)
discounting_q4_e5 <- discounting_e5 %>% 
  filter(FirstBilledDateFirstOfTheMonth >= as.Date("2020-04-01") & 
           FirstBilledDateFirstOfTheMonth <= as.Date("2020-06-30") ) %>% 
  group_by(TPID) %>% summarise(E5DiscountQ4 = 1- sum(ContractValue)/sum(ListPriceAmount)) %>% select(TPID,E5DiscountQ4)

discounting_data <- full_join(discounting_q1_overall, discounting_q2_overall, by = "TPID") %>% 
                    full_join(., discounting_q3_overall, by = "TPID") %>% 
                    full_join(., discounting_q4_overall, by = "TPID") %>% 
                    full_join(., discounting_q1_e5, by = "TPID") %>% 
                    full_join(., discounting_q2_e5, by = "TPID") %>% 
                    full_join(., discounting_q3_e5, by = "TPID") %>% 
                    full_join(., discounting_q4_e5, by = "TPID") 

discounting_data[is.na(discounting_data)] <- 0
discounting_data$TPID <- as.character(discounting_data$TPID)

acc_attributes <- left_join(acc_attributes, discounting_data, by = "TPID")
acc_attributes[is.na(acc_attributes)] <- 0

####### PAU ###########
# Features - Average PAU from FY20Q1 to FY20 Q4, PAU delta from Q1 to Q4

pau <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\PAU.csv")
pau$Date <- as.Date(pau$Date, "%m/%d/%Y")
pau <- pau %>% filter(Date > as.Date("2019-06-30") & Date <= as.Date("2020-06-30"))

pau_avg <- pau %>% group_by(TPID) %>% summarise(AverageEXOPAU = mean(EXOPaidAvailableUnits), 
                                                AverageProplusPAU = mean(ProPlusPaidAvailableUnits),
                                                AverageTeamsPAU = mean(TeamsPaidAvailableUnits),
                                                AverageODBPAU = mean(OD4BPaidAvailableUnits),
                                                AverageAADPPAU = mean(AADPPaidAvailableUnits),
                                                AverageODSPPAU = mean(ODSPPaidAvailableUnits),
                                                AverageSPOPAU = mean(SPOPaidAvailableUnits),
                                                AverageIntunePAU = mean(IntunePaidAvailableUnits))

pau_exo <- pau %>% select(TPID, Date, EXOPaidAvailableUnits) %>% spread(Date, EXOPaidAvailableUnits)
pau_exo$ExoPauFY20Q2Q1Delta <- pau_exo$`2019-12-31` - pau_exo$`2019-09-30` 
pau_exo$ExoPauFY20Q3Q2Delta <- pau_exo$`2020-03-31` - pau_exo$`2019-12-31` 
pau_exo$ExoPauFY20Q4Q3Delta <- pau_exo$`2020-06-30` - pau_exo$`2020-03-31` 
pau_exo <- pau_exo %>% select(TPID, ExoPauFY20Q2Q1Delta, ExoPauFY20Q3Q2Delta, ExoPauFY20Q4Q3Delta)

pau_pp <- pau %>% select(TPID, Date, ProPlusPaidAvailableUnits) %>% spread(Date, ProPlusPaidAvailableUnits)
pau_pp$ProplusPauFY20Q2Q1Delta <- pau_pp$`2019-12-31` - pau_pp$`2019-09-30` 
pau_pp$ProplusPauFY20Q3Q2Delta <- pau_pp$`2020-03-31` - pau_pp$`2019-12-31` 
pau_pp$ProplusPauFY20Q4Q3Delta <- pau_pp$`2020-06-30` - pau_pp$`2020-03-31` 
pau_pp <- pau_pp %>% select(TPID, ProplusPauFY20Q2Q1Delta, ProplusPauFY20Q3Q2Delta, ProplusPauFY20Q4Q3Delta)

pau_teams <- pau %>% select(TPID, Date, TeamsPaidAvailableUnits) %>% spread(Date, TeamsPaidAvailableUnits)
pau_teams$TeamsPauFY20Q2Q1Delta <- pau_teams$`2019-12-31` - pau_teams$`2019-09-30` 
pau_teams$TeamsPauFY20Q3Q2Delta <- pau_teams$`2020-03-31` - pau_teams$`2019-12-31` 
pau_teams$TeamsPauFY20Q4Q3Delta <- pau_teams$`2020-06-30` - pau_teams$`2020-03-31` 
pau_teams <- pau_teams %>% select(TPID, TeamsPauFY20Q2Q1Delta, TeamsPauFY20Q3Q2Delta, TeamsPauFY20Q4Q3Delta)

pau_odsp <- pau %>% select(TPID, Date, ODSPPaidAvailableUnits) %>% spread(Date, ODSPPaidAvailableUnits)
pau_odsp$OdspPauFY20Q2Q1Delta <- pau_odsp$`2019-12-31` - pau_odsp$`2019-09-30` 
pau_odsp$OdspPauFY20Q3Q2Delta <- pau_odsp$`2020-03-31` - pau_odsp$`2019-12-31` 
pau_odsp$OdspPauFY20Q4Q3Delta <- pau_odsp$`2020-06-30` - pau_odsp$`2020-03-31` 
pau_odsp <- pau_odsp %>% select(TPID, OdspPauFY20Q2Q1Delta, OdspPauFY20Q3Q2Delta, OdspPauFY20Q4Q3Delta)

pau_aadp <- pau %>% select(TPID, Date, AADPPaidAvailableUnits) %>% spread(Date, AADPPaidAvailableUnits)
pau_aadp$AADPPauFY20Q2Q1Delta <- pau_aadp$`2019-12-31` - pau_aadp$`2019-09-30` 
pau_aadp$AADPPauFY20Q3Q2Delta <- pau_aadp$`2020-03-31` - pau_aadp$`2019-12-31` 
pau_aadp$AADPPauFY20Q4Q3Delta <- pau_aadp$`2020-06-30` - pau_aadp$`2020-03-31` 
pau_aadp <- pau_aadp %>% select(TPID, AADPPauFY20Q2Q1Delta, AADPPauFY20Q3Q2Delta, AADPPauFY20Q4Q3Delta)

pau_intune <- pau %>% select(TPID, Date, IntunePaidAvailableUnits) %>% spread(Date, IntunePaidAvailableUnits)
pau_intune$IntunePauFY20Q2Q1Delta <- pau_intune$`2019-12-31` - pau_intune$`2019-09-30` 
pau_intune$IntunePauFY20Q3Q2Delta <- pau_intune$`2020-03-31` - pau_intune$`2019-12-31` 
pau_intune$IntunePauFY20Q4Q3Delta <- pau_intune$`2020-06-30` - pau_intune$`2020-03-31` 
pau_intune <- pau_intune %>% select(TPID, IntunePauFY20Q2Q1Delta, IntunePauFY20Q3Q2Delta, IntunePauFY20Q4Q3Delta)

pau_data <- left_join(pau_avg, pau_exo, by = "TPID") %>% 
  left_join(., pau_pp, by = "TPID") %>% 
  left_join(., pau_teams, by = "TPID") %>% 
  left_join(., pau_odsp, by = "TPID") %>% 
  left_join(., pau_aadp, by = "TPID") %>% 
  left_join(., pau_intune, by = "TPID")

pau_data$TPID <- as.character(pau_data$TPID)
acc_attributes <- left_join(acc_attributes, pau_data, by = "TPID")

######## MAU ##########
#Features - As treatment - delta in FY20 H2, growth in FY20 H2, ind. workloads, sum of workloads, O365
# Features - History of usage 
mau <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\MAU\\MAU.csv") %>% 
        select(TPID, Date, EXO_MAU, ODB_MAU, OfficeClientAllUp_MAU, SPO_MAU, TeamsAllUp_MAU)
mau$Date <- as.Date(mau$Date, "%m/%d/%Y")

intune_mau <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\MAU\\Intune_MAU.csv") %>% 
  select(TPId, Date, Intune_MAU) 
colnames(intune_mau)[1] <- "TPID"
intune_mau$Date <- as.Date(intune_mau$Date, "%m/%d/%Y")

aadp_mau <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\MAU\\AADP_MAU.csv") %>% 
  select(TPId, Date, AADP_MAU) 
colnames(aadp_mau)[1] <- "TPID"
aadp_mau$Date <- as.Date(aadp_mau$Date, "%m/%d/%Y")

odsp_mau <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\MAU\\ODSP_MAU.csv") %>% 
  select(TPId, Date, odsp_MAU) 
colnames(odsp_mau)[1] <- "TPID" 
odsp_mau$Date <- as.Date(odsp_mau$Date, "%m/%d/%Y")

o365_mau <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\MAU\\O365_MAU.csv") %>% 
  select(TPId, Date, o365_MAU) 
colnames(o365_mau)[1] <- "TPID" 
o365_mau$Date <- as.Date(o365_mau$Date, "%m/%d/%Y")

mau_data <- full_join(mau, intune_mau, by = c("TPID", "Date")) %>% 
  full_join(., aadp_mau, by = c("TPID", "Date")) %>% 
  full_join(., odsp_mau, by = c("TPID", "Date")) %>%
  full_join(., o365_mau, by = c("TPID", "Date")) 

mau_data[is.na(mau_data)] <- 0
mau_data$O365SumMau <- mau_data$EXO_MAU + mau_data$odsp_MAU + mau_data$OfficeClientAllUp_MAU + mau_data$TeamsAllUp_MAU
mau_data$SecSumMau <- mau_data$Intune_MAU + mau_data$AADP_MAU
mau_data$O365SecSumMau <- mau_data$O365SumMau + mau_data$SecSumMau

# Per workload and combination
is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))

mau_exo <- mau_data %>% select(TPID, Date, EXO_MAU) %>% spread(Date, EXO_MAU)
mau_exo[is.na(mau_exo)] <- 0
mau_exo$ExoDeltaTreatment <- mau_exo$`2020-12-31` - mau_exo$`2020-06-30`
mau_exo$ExoGrowthTreatment <- (mau_exo$`2020-12-31` - mau_exo$`2020-06-30`)/mau_exo$`2020-06-30`
# Usage pre treatment
mau_exo$ExoDeltaSep19Dec19 <- mau_exo$`2019-12-31` - mau_exo$`2019-09-30`
mau_exo$ExoDeltaJun19Sep19 <- mau_exo$`2019-09-30` - mau_exo$`2019-06-30`
mau_exo$ExoDeltaMar19Jun19 <- mau_exo$`2019-06-30` - mau_exo$`2019-03-31`
mau_exo$ExoDeltaJan19Mar19 <- mau_exo$`2019-03-31` - mau_exo$`2019-01-31`
mau_exo <- mau_exo %>% select(TPID, ExoDeltaTreatment, ExoGrowthTreatment, ExoDeltaSep19Dec19, ExoDeltaJun19Sep19, ExoDeltaMar19Jun19, ExoDeltaJan19Mar19)
mau_exo[is.nan(mau_exo)] <- 0

mau_odsp <- mau_data %>% select(TPID, Date, odsp_MAU) %>% spread(Date, odsp_MAU)
mau_odsp[is.na(mau_odsp)] <- 0
mau_odsp$OdspDeltaTreatment <- mau_odsp$`2020-12-31` - mau_odsp$`2020-06-30`
mau_odsp$OdspGrowthTreatment <- (mau_odsp$`2020-12-31` - mau_odsp$`2020-06-30`)/mau_odsp$`2020-06-30`
mau_odsp$OdspDeltaSep19Dec19 <- mau_odsp$`2019-12-31` - mau_odsp$`2019-09-30`
mau_odsp$OdspDeltaJun19Sep19 <- mau_odsp$`2019-09-30` - mau_odsp$`2019-06-30`
mau_odsp$OdspDeltaMar19Jun19 <- mau_odsp$`2019-06-30` - mau_odsp$`2019-03-31`
mau_odsp$OdspDeltaJan19Mar19 <- mau_odsp$`2019-03-31` - mau_odsp$`2019-01-31`
mau_odsp <- mau_odsp %>% select(TPID, OdspDeltaTreatment, OdspGrowthTreatment, OdspDeltaSep19Dec19, OdspDeltaJun19Sep19, OdspDeltaMar19Jun19, OdspDeltaJan19Mar19)
mau_odsp[is.nan(mau_odsp)] <- 0

mau_proplus <- mau_data %>% select(TPID, Date, OfficeClientAllUp_MAU) %>% spread(Date, OfficeClientAllUp_MAU)
mau_proplus[is.na(mau_proplus)] <- 0
mau_proplus$ProplusDeltaTreatment <- mau_proplus$`2020-12-31` - mau_proplus$`2020-06-30`
mau_proplus$ProplusGrowthTreatment <- (mau_proplus$`2020-12-31` - mau_proplus$`2020-06-30`)/mau_proplus$`2020-06-30`
mau_proplus$ProplusDeltaSep19Dec19 <- mau_proplus$`2019-12-31` - mau_proplus$`2019-09-30`
mau_proplus$ProplusDeltaJun19Sep19 <- mau_proplus$`2019-09-30` - mau_proplus$`2019-06-30`
mau_proplus$ProplusDeltaMar19Jun19 <- mau_proplus$`2019-06-30` - mau_proplus$`2019-03-31`
mau_proplus$ProplusDeltaJan19Mar19 <- mau_proplus$`2019-03-31` - mau_proplus$`2019-01-31`
mau_proplus <- mau_proplus %>% select(TPID, ProplusDeltaTreatment, ProplusGrowthTreatment, ProplusDeltaSep19Dec19, ProplusDeltaJun19Sep19, ProplusDeltaMar19Jun19, ProplusDeltaJan19Mar19)
mau_proplus[is.nan(mau_proplus)] <- 0

mau_teams <- mau_data %>% select(TPID, Date, TeamsAllUp_MAU) %>% spread(Date, TeamsAllUp_MAU)
mau_teams[is.na(mau_teams)] <- 0
mau_teams$TeamsDeltaTreatment <- mau_teams$`2020-12-31` - mau_teams$`2020-06-30`
mau_teams$TeamsGrowthTreatment <- (mau_teams$`2020-12-31` - mau_teams$`2020-06-30`)/mau_teams$`2020-06-30`
mau_teams$TeamsDeltaSep19Dec19 <- mau_teams$`2019-12-31` - mau_teams$`2019-09-30`
mau_teams$TeamsDeltaJun19Sep19 <- mau_teams$`2019-09-30` - mau_teams$`2019-06-30`
mau_teams$TeamsDeltaMar19Jun19 <- mau_teams$`2019-06-30` - mau_teams$`2019-03-31`
mau_teams$TeamsDeltaJan19Mar19 <- mau_teams$`2019-03-31` - mau_teams$`2019-01-31`
mau_teams <- mau_teams %>% select(TPID, TeamsDeltaTreatment, TeamsGrowthTreatment, TeamsDeltaSep19Dec19, TeamsDeltaJun19Sep19, TeamsDeltaMar19Jun19, TeamsDeltaJan19Mar19)
mau_teams[is.nan(mau_teams)] <- 0

mau_aadp <- mau_data %>% select(TPID, Date, AADP_MAU) %>% spread(Date, AADP_MAU)
mau_aadp[is.na(mau_aadp)] <- 0
mau_aadp$AADPDeltaTreatment <- mau_aadp$`2020-12-31` - mau_aadp$`2020-06-30`
mau_aadp$AADPGrowthTreatment <- (mau_aadp$`2020-12-31` - mau_aadp$`2020-06-30`)/mau_aadp$`2020-06-30`
mau_aadp$AADPDeltaSep19Dec19 <- mau_aadp$`2019-12-31` - mau_aadp$`2019-09-30`
mau_aadp$AADPDeltaJun19Sep19 <- mau_aadp$`2019-09-30` - mau_aadp$`2019-06-30`
mau_aadp$AADPDeltaMar19Jun19 <- mau_aadp$`2019-06-30` - mau_aadp$`2019-03-31`
mau_aadp$AADPDeltaJan19Mar19 <- mau_aadp$`2019-03-31` - mau_aadp$`2019-01-31`
mau_aadp <- mau_aadp %>% select(TPID, AADPDeltaTreatment, AADPGrowthTreatment, AADPDeltaSep19Dec19, AADPDeltaJun19Sep19, AADPDeltaMar19Jun19, AADPDeltaJan19Mar19)
mau_aadp[is.nan(mau_aadp)] <- 0

mau_intune <- mau_data %>% select(TPID, Date, Intune_MAU) %>% spread(Date, Intune_MAU)
mau_intune[is.na(mau_intune)] <- 0
mau_intune$IntuneDeltaTreatment <- mau_intune$`2020-12-31` - mau_intune$`2020-06-30`
mau_intune$IntuneGrowthTreatment <- (mau_intune$`2020-12-31` - mau_intune$`2020-06-30`)/mau_intune$`2020-06-30`
mau_intune$IntuneDeltaNov19Dec19 <- mau_intune$`2019-12-31` - mau_intune$`2019-11-30`
# mau_intune$IntuneDeltaJun19Sep19 <- mau_intune$`2019-09-30` - mau_intune$`2019-06-30` # No history
# mau_intune$IntuneDeltaMar19Jun19 <- mau_intune$`2019-06-30` - mau_intune$`2019-03-31`
# mau_intune$IntuneDeltaJan19Mar19 <- mau_intune$`2019-03-31` - mau_intune$`2019-01-31`
mau_intune <- mau_intune %>% select(TPID, IntuneDeltaTreatment, IntuneGrowthTreatment, IntuneDeltaNov19Dec19)
mau_intune[is.nan(mau_intune)] <- 0

mau_o365sum <- mau_data %>% select(TPID, Date, O365SumMau) %>% spread(Date, O365SumMau)
mau_o365sum[is.na(mau_o365sum)] <- 0
mau_o365sum$O365SumDeltaTreatment <- mau_o365sum$`2020-12-31` - mau_o365sum$`2020-06-30`
mau_o365sum$O365SumGrowthTreatment <- (mau_o365sum$`2020-12-31` - mau_o365sum$`2020-06-30`)/mau_o365sum$`2020-06-30`
mau_o365sum$O365SumDeltaSep19Dec19 <- mau_o365sum$`2019-12-31` - mau_o365sum$`2019-09-30`
mau_o365sum$O365SumDeltaJun19Sep19 <- mau_o365sum$`2019-09-30` - mau_o365sum$`2019-06-30`
mau_o365sum$O365SumDeltaMar19Jun19 <- mau_o365sum$`2019-06-30` - mau_o365sum$`2019-03-31`
mau_o365sum$O365SumDeltaJan19Mar19 <- mau_o365sum$`2019-03-31` - mau_o365sum$`2019-01-31`
mau_o365sum <- mau_o365sum %>% select(TPID, O365SumDeltaTreatment, O365SumGrowthTreatment, O365SumDeltaSep19Dec19, O365SumDeltaJun19Sep19, O365SumDeltaMar19Jun19, O365SumDeltaJan19Mar19)
mau_o365sum[is.nan(mau_o365sum)] <- 0

mau_o365secsum <- mau_data %>% select(TPID, Date, O365SecSumMau) %>% spread(Date, O365SecSumMau)
mau_o365secsum[is.na(mau_o365secsum)] <- 0
mau_o365secsum$O365SecSumDeltaTreatment <- mau_o365secsum$`2020-12-31` - mau_o365secsum$`2020-06-30`
mau_o365secsum$O365SecSumGrowthTreatment <- (mau_o365secsum$`2020-12-31` - mau_o365secsum$`2020-06-30`)/mau_o365secsum$`2020-06-30`
mau_o365secsum$O365SecSumDeltaSep19Dec19 <- mau_o365secsum$`2019-12-31` - mau_o365secsum$`2019-09-30`
mau_o365secsum$O365SecSumDeltaJun19Sep19 <- mau_o365secsum$`2019-09-30` - mau_o365secsum$`2019-06-30`
mau_o365secsum$O365SecSumDeltaMar19Jun19 <- mau_o365secsum$`2019-06-30` - mau_o365secsum$`2019-03-31`
mau_o365secsum$O365SecSumDeltaJan19Mar19 <- mau_o365secsum$`2019-03-31` - mau_o365secsum$`2019-01-31`
mau_o365secsum <- mau_o365secsum %>% select(TPID, O365SecSumDeltaTreatment, O365SecSumGrowthTreatment, O365SecSumDeltaSep19Dec19, O365SecSumDeltaJun19Sep19, O365SecSumDeltaMar19Jun19, O365SecSumDeltaJan19Mar19)
mau_o365secsum[is.nan(mau_o365secsum)] <- 0

mau_sec <- mau_data %>% select(TPID, Date, SecSumMau) %>% spread(Date, SecSumMau)
mau_sec[is.na(mau_sec)] <- 0
mau_sec$SecSumDeltaTreatment <- mau_sec$`2020-12-31` - mau_sec$`2020-06-30`
mau_sec$SecSumGrowthTreatment <- (mau_sec$`2020-12-31` - mau_sec$`2020-06-30`)/mau_sec$`2020-06-30`
mau_sec$SecSumDeltaNov19Dec19 <- mau_sec$`2019-12-31` - mau_sec$`2019-11-30`
# mau_sec$SecSumDeltaJun19Sep19 <- mau_sec$`2019-09-30` - mau_sec$`2019-06-30` # No intune history
# mau_sec$SecSumDeltaMar19Jun19 <- mau_sec$`2019-06-30` - mau_sec$`2019-03-31`
# mau_sec$SecSumDeltaJan19Mar19 <- mau_sec$`2019-03-31` - mau_sec$`2019-01-31`
mau_sec <- mau_sec %>% select(TPID, SecSumDeltaTreatment, SecSumGrowthTreatment, SecSumDeltaNov19Dec19)
mau_sec[is.nan(mau_sec)] <- 0

all_mau <- full_join(mau_exo, mau_odsp, by = "TPID") %>% 
  full_join(., mau_proplus, by = "TPID") %>% 
  full_join(., mau_teams, by = "TPID") %>% 
  full_join(., mau_aadp, by = "TPID") %>% 
  full_join(., mau_intune, by = "TPID") %>% 
  full_join(., mau_o365sum, by = "TPID") %>% 
  full_join(., mau_o365secsum, by = "TPID") %>% 
  full_join(., mau_sec, by = "TPID")

all_mau[is.na(all_mau)] <- 0
acc_attributes <- left_join(acc_attributes, all_mau, by = "TPID")

######## Usage  - TODO ##########




######## Licenses ##########
#Feature - delta for each quarter from 2019 to 2020 jun
licenses <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\Licenses.csv") 
licenses$Date <- as.Date(licenses$Date, "%m/%d/%Y")

licenses$onPremSeats <- licenses$Annuity + licenses$NonAnnuity + licenses$DarkAnnuity
licenses$E5Seats <- licenses$M365E5Seats + licenses$O365PlanE5Seats + licenses$EMSE5Seats + # covers most of the E5 suites and E5 standalones
                    licenses$ATPP2Seats + licenses$AADPremP2Seats + licenses$CloudAppSecuritySeats + 
                    licenses$AdvancedComplianceSeats + licenses$AdvancedSecurityManagementSeats + licenses$MyAnalyticsSeats + 
                    licenses$PowerBIPremiumSeats + licenses$PowerBIStandaloneSeats + licenses$PowerBIOfficeSuiteSeats
licenses$E3Seats <- licenses$M365E3Seats + licenses$O365PlanE3Seats + licenses$EMSE3Seats


lic_onPremSeats <- licenses %>% select(TPID, Date, onPremSeats) %>% spread(Date, onPremSeats)
lic_onPremSeats[is.na(lic_onPremSeats)] <- 0
lic_onPremSeats$OnPremLicDeltaMar20Jun20 <- lic_onPremSeats$`2020-06-30` - lic_onPremSeats$`2020-03-31`
lic_onPremSeats$OnPremLicDeltaJan20Mar20 <- lic_onPremSeats$`2020-03-31` - lic_onPremSeats$`2019-12-31`
lic_onPremSeats$OnPremLicDeltaSep19Dec19 <- lic_onPremSeats$`2019-12-31` - lic_onPremSeats$`2019-09-30`
lic_onPremSeats$OnPremLicDeltaJun19Sep19 <- lic_onPremSeats$`2019-09-30` - lic_onPremSeats$`2019-06-30`
lic_onPremSeats$OnPremLicDeltaMar19Jun19 <- lic_onPremSeats$`2019-06-30` - lic_onPremSeats$`2019-03-31`
lic_onPremSeats$OnPremLicDeltaJan19Mar19 <- lic_onPremSeats$`2019-03-31` - lic_onPremSeats$`2019-01-31`
lic_onPremSeats <- lic_onPremSeats %>% select(TPID, OnPremLicDeltaMar20Jun20, OnPremLicDeltaJan20Mar20, 
                                              OnPremLicDeltaSep19Dec19, OnPremLicDeltaJun19Sep19, OnPremLicDeltaMar19Jun19, OnPremLicDeltaJan19Mar19)
lic_onPremSeats[is.nan(lic_onPremSeats)] <- 0


lic_E5Seats <- licenses %>% select(TPID, Date, E5Seats) %>% spread(Date, E5Seats)
lic_E5Seats[is.na(lic_E5Seats)] <- 0
lic_E5Seats$E5LicDeltaMar20Jun20 <- lic_E5Seats$`2020-06-30` - lic_E5Seats$`2020-03-31`
lic_E5Seats$E5LicDeltaJan20Mar20 <- lic_E5Seats$`2020-03-31` - lic_E5Seats$`2019-12-31`
lic_E5Seats$E5LicDeltaSep19Dec19 <- lic_E5Seats$`2019-12-31` - lic_E5Seats$`2019-09-30`
lic_E5Seats$E5LicDeltaJun19Sep19 <- lic_E5Seats$`2019-09-30` - lic_E5Seats$`2019-06-30`
lic_E5Seats$E5LicDeltaMar19Jun19 <- lic_E5Seats$`2019-06-30` - lic_E5Seats$`2019-03-31`
lic_E5Seats$E5LicDeltaJan19Mar19 <- lic_E5Seats$`2019-03-31` - lic_E5Seats$`2019-01-31`
lic_E5Seats <- lic_E5Seats %>% select(TPID, E5LicDeltaMar20Jun20, E5LicDeltaJan20Mar20, 
                                              E5LicDeltaSep19Dec19, E5LicDeltaJun19Sep19, E5LicDeltaMar19Jun19, E5LicDeltaJan19Mar19)
lic_E5Seats[is.nan(lic_E5Seats)] <- 0

lic_E3Seats <- licenses %>% select(TPID, Date, E3Seats) %>% spread(Date, E3Seats)
lic_E3Seats[is.na(lic_E3Seats)] <- 0
lic_E3Seats$E3LicDeltaMar20Jun20 <- lic_E3Seats$`2020-06-30` - lic_E3Seats$`2020-03-31`
lic_E3Seats$E3LicDeltaJan20Mar20 <- lic_E3Seats$`2020-03-31` - lic_E3Seats$`2019-12-31`
lic_E3Seats$E3LicDeltaSep19Dec19 <- lic_E3Seats$`2019-12-31` - lic_E3Seats$`2019-09-30`
lic_E3Seats$E3LicDeltaJun19Sep19 <- lic_E3Seats$`2019-09-30` - lic_E3Seats$`2019-06-30`
lic_E3Seats$E3LicDeltaMar19Jun19 <- lic_E3Seats$`2019-06-30` - lic_E3Seats$`2019-03-31`
lic_E3Seats$E3LicDeltaJan19Mar19 <- lic_E3Seats$`2019-03-31` - lic_E3Seats$`2019-01-31`
lic_E3Seats <- lic_E3Seats %>% select(TPID, E3LicDeltaMar20Jun20, E3LicDeltaJan20Mar20, 
                                      E3LicDeltaSep19Dec19, E3LicDeltaJun19Sep19, E3LicDeltaMar19Jun19, E3LicDeltaJan19Mar19)
lic_E3Seats[is.nan(lic_E3Seats)] <- 0

all_licenses <- full_join(lic_onPremSeats, lic_E5Seats, by = "TPID") %>% 
  full_join(., lic_E3Seats, by = "TPID")

all_licenses[is.na(all_licenses)] <- 0
acc_attributes <- left_join(acc_attributes, all_licenses, by = "TPID")

######## E5 Revenue - TODO ##########
# Features - Revenue delta, revenue growth 
e5_rev <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\UsageToRevenue\\E5Revenue.csv")
colnames(e5_rev)[1] <- "TPID"
e5_rev[is.na(e5_rev)] <- 0




######## Non E5 Revenue  - TODO ##########











