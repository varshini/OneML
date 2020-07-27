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

calculateUsageRatios <- function(profile, usage)
{
  usage_profile <- left_join(profile, usage, by = c("FinalTPID", "ym")) 
  usage_profile[is.na(usage_profile)] <- 0
  
  # O365 Usage Ratios
  usage_profile$EXOUsagePaidPerc <- usage_profile$EXO/usage_profile$EXOPaidAvailableUnits
  usage_profile$EXOUsageEnabledPerc <- usage_profile$EXO/usage_profile$EXOEnabledUsers
  usage_profile$SPOUsagePaidPerc <- usage_profile$SPO/usage_profile$SPOPaidAvailableUnits
  usage_profile$SPOUsageEnabledPerc <- usage_profile$SPO/usage_profile$SPOEnabledUsers
  usage_profile$ODBUsagePaidPerc <- usage_profile$ODB/usage_profile$OD4BPaidAvailableUnits
  usage_profile$ODBUsageEnabledPerc <- usage_profile$ODB/usage_profile$OD4BEnabledUsers
  usage_profile$ProPlusUsagePaidPerc <- usage_profile$ProPlusMAU/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$DesktopProPlusUsagePaidPerc <- usage_profile$DesktopProPlusMAU/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$PerpetualProPlusUsagePaidPerc <- usage_profile$DesktopPerpetualMAU/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$DesktopSubscriptionProPlusUsagePaidPerc <- usage_profile$DesktopSubscriptionMAU/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$DesktopWebProPlusUsagePaidPerc <- usage_profile$DesktopWebMAU/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$TeamsUsagePaidPerc <- usage_profile$TeamsMau/usage_profile$TeamsPaidAvailableUnits 
  usage_profile$TeamsDesktopUsagePaidPerc <- usage_profile$TeamsDesktopMAU/usage_profile$TeamsPaidAvailableUnits
  usage_profile$TeamsMobileUsagePaidPerc <- usage_profile$TeamsMobileMAU/usage_profile$TeamsPaidAvailableUnits
  usage_profile$TeamsAppThirdPartyUsagePaidPerc <- usage_profile$SideloadedAppThirdParty/usage_profile$TeamsPaidAvailableUnits
  usage_profile$TeamsAppLOBUsagePaidPerc <- usage_profile$SideloadedAppLOB/usage_profile$TeamsPaidAvailableUnits
  usage_profile$TeamsAppUnknownUsagePaidPerc <- usage_profile$SideloadedAppUnknown/usage_profile$TeamsPaidAvailableUnits
  usage_profile$TeamsAppSecondPartyUsagePaidPerc <- usage_profile$SecondParty/usage_profile$TeamsPaidAvailableUnits

  #EMS Usage Ratios 
  usage_profile$IntuneUsagePaidPerc <- usage_profile$IntuneMAD/usage_profile$IntunePaidAvailableUnits
  usage_profile$IntuneUsageEnabledPerc <- usage_profile$IntuneMAD/usage_profile$IntuneEnabledUsers
  usage_profile$AADPUsagePaidPerc <- usage_profile$AADPUsageCount/usage_profile$AADPPaidAvailableUnits
  usage_profile$B2BUsagePaidPerc <- usage_profile$B2BActiveUniqueUserCount/usage_profile$AADPPaidAvailableUnits
  usage_profile$CAAUsagePaidPerc <- usage_profile$CAActiveUniqueUserCount/usage_profile$AADPPaidAvailableUnits
  usage_profile$MCASUsagePaidPerc <- usage_profile$MCAS/usage_profile$MCASPaidAvailableUnits
  usage_profile$MIGUsagePaidPerc <- usage_profile$MIG/usage_profile$MIGPaidAvailableUnits
  usage_profile$MIPUsagePaidPerc <- usage_profile$MIP/usage_profile$MIPPaidAvailableUnits
  usage_profile$OATPUsagePaidPerc <- usage_profile$OATP/usage_profile$OATPPaidAvailableUnits

  usage_profile[sapply(usage_profile, simplify = 'matrix', is.infinite)] <- 0 # Checked the distribution, mostly very small MAU like 1 till 90th perc. 
  usage_profile[sapply(usage_profile, simplify = 'matrix', is.na)] <- 0
  usage_profile[sapply(usage_profile, simplify = 'matrix', is.nan)] <- 0
  
  usage_profile[ , 163:185 ][ usage_profile[ , 163:185  ] > 1 ] <- 1 
  
  # convert logical columns to numeric
  
  usage_profile$HasTrial <- as.integer(as.logical(usage_profile$HasTrial))
  usage_profile$HasM365 <- as.integer(as.logical(usage_profile$HasM365))
  usage_profile$HasM365SKUBusiness <- as.integer(as.logical(usage_profile$HasM365SKUBusiness))
  usage_profile$HasM365SKUF1 <- as.integer(as.logical(usage_profile$HasM365SKUF1))
  usage_profile$HasM365SKUE3 <- as.integer(as.logical(usage_profile$HasM365SKUE3))
  usage_profile$HasM365SKUE5 <- as.integer(as.logical(usage_profile$HasM365SKUE5))
  usage_profile$HasEMSSku <- as.integer(as.logical(usage_profile$HasEMSSku))
  usage_profile$HasM365Sku <- as.integer(as.logical(usage_profile$HasM365Sku))
  usage_profile$HasOffice365Sku <- as.integer(as.logical(usage_profile$HasOffice365Sku))
  usage_profile$HasWindowsSku <- as.integer(as.logical(usage_profile$HasWindowsSku))
  usage_profile$HasDynamicsSku <- as.integer(as.logical(usage_profile$HasDynamicsSku))
  usage_profile$HasOfficeSKUE1 <- as.integer(as.logical(usage_profile$HasOfficeSKUE1))
  usage_profile$HasOfficeSKUE3 <- as.integer(as.logical(usage_profile$HasOfficeSKUE3))
  usage_profile$HasOfficeSKUE4 <- as.integer(as.logical(usage_profile$HasOfficeSKUE4))
  usage_profile$HasOfficeSKUE5 <- as.integer(as.logical(usage_profile$HasOfficeSKUE5))
  usage_profile$TenantHasO365TrialInMonth <- as.integer(as.logical(usage_profile$TenantHasO365TrialInMonth))
  usage_profile$TenantHasEMSTrialInMonth <- as.integer(as.logical(usage_profile$TenantHasEMSTrialInMonth))
  usage_profile$TenantHasM365TrialInMonth <- as.integer(as.logical(usage_profile$TenantHasM365TrialInMonth))
  usage_profile$IsFastTrackTenant <- as.integer(as.logical(usage_profile$IsFastTrackTenant))
  
  return(usage_profile)
}

##### TRAINING ###########

# Tenant Profile #
tenant_profile <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\TenantProfile\\Training_TenantProfile.csv")

# Usage, combined with above #
usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Training_Usage.csv")
usage_tenantprofile <- calculateUsageRatios(tenant_profile, usage)
usage_tenantprofile$ym <- "201912"

# Perpetual Usage #
perp_usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Training_Perpetual.csv")
perp_usage$ym <- "201912"

# Licenses #
cloud_licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Training_Seats.csv")
cloud_licenses$ym <- "201912"
onprem_licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Training_OnPremSeats.csv")
onprem_licenses$ym <- "201912"

# Compete #
compete <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Compete\\CompeteSignal_12_31_2019.csv")
compete$ym <- "201912"
colnames(compete)[1] <- "FinalTPID"

# Geo, Indus #
geo_indus <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\MSXiCohorts.xlsx")
geo_indus <- geo_indus %>% select(TPID, Industry, Subsidiary)
colnames(geo_indus)[1] <- "FinalTPID"
geo_indus <- left_join(smctpids, geo_indus, by = "FinalTPID")
geo_indus$Industry[is.na(geo_indus$Industry)] <- "N/A"
geo_indus$Subsidiary[is.na(geo_indus$Subsidiary)] <- "N/A"
geo_indus$Area <- as.factor(geo_indus$Subsidiary)
geo_indus$Area <- as.numeric(geo_indus$Area)
geo_indus$IndustryNum <- as.factor(geo_indus$Industry)
geo_indus$IndustryNum <- as.numeric(geo_indus$IndustryNum)
geo_mapping <- geo_indus %>% distinct(Subsidiary, Area) 
indus_mapping <- geo_indus %>% distinct(Industry, IndustryNum)
geo_indus <- geo_indus %>% select(FinalTPID, Area, IndustryNum)
geo_indus$ym <- "201912"

# Revenue #
revenue <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Revenue\\Training_Revenue.csv")
revenue$ym <- "201912"
revenue$FinalTPID <- as.integer(revenue$FinalTPID)

# Training Labels #
training_labels <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\FY21TrainingLabelsFinal.csv")
training_labels$ym <- "201912"

# Put together the final data stream # 
training_data <- training_labels %>% left_join(usage_tenantprofile, by = c("FinalTPID", "ym")) %>% 
  left_join(cloud_licenses, by = c("FinalTPID", "ym")) %>% 
  left_join(onprem_licenses, by = c("FinalTPID", "ym")) %>% 
  left_join(compete, by = c("FinalTPID", "ym")) %>% 
  left_join(perp_usage, by = c("FinalTPID", "ym")) %>% 
  left_join(geo_indus, by = c("FinalTPID", "ym")) %>% 
  left_join(revenue, by = c("FinalTPID", "ym"))

training_data[sapply(training_data, simplify = 'matrix', is.infinite)] <- 0
training_data[sapply(training_data, simplify = 'matrix', is.nan)] <- 0
training_data[sapply(training_data, simplify = 'matrix', is.na)] <- 0

training_data <-  subset(training_data, select = -c(SnapshotDate, FirstPaidStartDate) )
write.csv(training_data, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\TrainingFeaturesFinalv3.csv", row.names = FALSE)


##### SCORING ###########

# Tenant Profile #
tenant_profile <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\TenantProfile\\Scoring_TenantProfile.csv")

# Usage, combined with above #
usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Scoring_Usage.csv")
usage_tenantprofile <- calculateUsageRatios(tenant_profile, usage)
usage_tenantprofile$ym <- "202006"

# Perpetual Usage #
perp_usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Scoring_Perpetual.csv")
perp_usage$ym <- "202006"

# Licenses #
cloud_licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Scoring_Seats.csv")
cloud_licenses$ym <- "202006"
onprem_licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Seats\\Scoring_OnPremSeats.csv")
onprem_licenses$ym <- "202006"

# Compete #
compete <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Compete\\CompeteSignal_06_30_2020.csv")
compete$ym <- "202006"
colnames(compete)[1] <- "FinalTPID"

# Geo, Indus #
geo_indus <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\MSXiCohorts.xlsx")
geo_indus <- geo_indus %>% select(TPID, Industry, Subsidiary)
geo_indus1 <- left_join(geo_indus, geo_mapping) %>% select(TPID, Area)
geo_indus1$Area[is.na(geo_indus1$Area)] <- 70
geo_indus2 <- left_join(geo_indus, indus_mapping) %>% select(TPID, IndustryNum)
geo_indus2$IndustryNum[is.na(geo_indus2$IndustryNum)] <- 17
geo_indus3 <- inner_join(geo_indus1, geo_indus2)
geo_indus3$ym <- "202006"
colnames(geo_indus3)[1] <- "FinalTPID"

# Revenue #
revenue <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Revenue\\Scoring_Revenue.csv")
revenue$ym <- "202006"
revenue$FinalTPID <- as.integer(revenue$FinalTPID)

# Scoring cohort - All SMC TPIDs can show either conversion or expansion#
smctpids <- smctpids %>% select(FinalTPID)
smctpids$ym <- "202006"

# Put together the final data stream # 
scoring_data <- smctpids %>% left_join(usage_tenantprofile, by = c("FinalTPID", "ym")) %>% 
  left_join(cloud_licenses, by = c("FinalTPID", "ym")) %>% 
  left_join(onprem_licenses, by = c("FinalTPID", "ym")) %>% 
  left_join(compete, by = c("FinalTPID", "ym")) %>% 
  left_join(perp_usage, by = c("FinalTPID", "ym")) %>% 
  left_join(geo_indus3, by = c("FinalTPID", "ym")) %>% 
  left_join(revenue, by = c("FinalTPID", "ym"))

scoring_data[sapply(scoring_data, simplify = 'matrix', is.infinite)] <- 0
scoring_data[sapply(scoring_data, simplify = 'matrix', is.nan)] <- 0
scoring_data[sapply(scoring_data, simplify = 'matrix', is.na)] <- 0

scoring_data <-  subset(scoring_data, select = -c(SnapshotDate, FirstPaidStartDate) )

write.csv(scoring_data, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\ScoringFeaturesFinalv3.csv", row.names = FALSE)
