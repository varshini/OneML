library(readxl)
library(dplyr)
library(tidyverse)

# Aggregate all training and scoring features into one file for modeling #
# For analysis purposes, needs some more refining of the dataset since lot of outliers and anomalies #

smcc_accounts <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\FY20SMCCList.xlsx")
colnames(smcc_accounts)[1] <- "FinalTPID"
smcc_accounts$FinalTPID <- as.numeric(smcc_accounts$FinalTPID)

target_list <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\TargetList.xlsx", sheet = "TPIDList")
target_list$CustomerAdd <- 1
target_list$ym <- "FY20Q1"
  
# Training #
aggregate <- function(labels, tenantprofile, collab, o365, ems, geo, licenses, revenue)
{
  #labels - join with SMCC accounts 
  
  labels <- labels %>% inner_join(smcc_accounts, by = "FinalTPID")
  
  #MAU/Paid, MAU/Enabled, Enabled/Paid
  #Not doing Enabled/Paid , very messed up percentages - 
  # if needed you can use those percentages (will make inf to 0 and cap at 100%)
  
  tenantprofile <- inner_join(labels, tenantprofile, by = c("FinalTPID", "ym"))
  tenantprofile[sapply(tenantprofile, simplify = 'matrix', is.infinite)] <- 0
  tenantprofile[ , 77:104 ][ tenantprofile[ , 77:104 ] > 1 ] <- 1
  tenantprofile <-  subset(tenantprofile, select = -c(SnapshotDate,FirstPaidStartDate) )
  tenantprofile$IsFastTrackTenant <- as.numeric(as.logical(tenantprofile$IsFastTrackTenant))
  
  # Collab 
  collab$WXPOEditorsPerc <- collab$NumWXPOEditors/collab$NumWXPOUsers
  collab$WXPOCoauthorsPerc <- collab$NumWXPOCoauthors/collab$NumWXPOUsers
  collab$WXPOCollaboratorsPerc <- collab$NumWXPOUsersCollaborated/collab$NumWXPOUsers
  collab <- collab %>% dplyr::select (FinalTPID, ym, WXPOEditorsPerc, WXPOCoauthorsPerc, WXPOCollaboratorsPerc)
  collab[is.na(collab)] <- 0
  collab[sapply(collab, simplify = 'matrix', is.infinite)] <- 0
  
  # Usage - MAU/Paid, MAU/Enabled 
  usage_profile <- full_join(o365_usage, tenantprofile, by = c("FinalTPID", "ym")) %>% 
    full_join(., ems_usage, by = c("FinalTPID", "ym")) 
  
  usage_profile[is.na(usage_profile)] <- 0
  
  #Compute usage percentages 
  usage_profile$EXOUsagePaidPerc <- usage_profile$EXOMAU/usage_profile$EXOPaidAvailableUnits
  usage_profile$EXOUsageEnabledPerc <- usage_profile$EXOMAU/usage_profile$EXOEnabledUsers
  usage_profile$SPOUsagePaidPerc <- usage_profile$ODSPMAU/usage_profile$SPOPaidAvailableUnits
  usage_profile$SPOUsageEnabledPerc <- usage_profile$ODSPMAU/usage_profile$SPOEnabledUsers
  usage_profile$SFBUsagePaidPerc <- usage_profile$SfBMAU/usage_profile$SfbPaidAvailableUnits
  usage_profile$SFBUsageEnabledPerc <- usage_profile$SfBMAU/usage_profile$SFBEnabledUsers
  usage_profile$ProPlusUsagePaidPerc <- usage_profile$ProPlusMAU/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$ProplusUsageEnabledPerc <- usage_profile$ProPlusMAU/usage_profile$PPDEnabledUsers
  usage_profile$TeamsUsagePaidPerc <- usage_profile$TeamsMAU/usage_profile$TeamsPaidAvailableUnits 
  usage_profile$TeamsUsageEnabledPerc <- usage_profile$TeamsMAU/usage_profile$TeamsFreemiumEnabledUsers
  usage_profile$YammerUsagePaidPerc <- usage_profile$YammerMAU/usage_profile$YammerPaidAvailableUnits
  usage_profile$YammerUsageEnabledPerc <- usage_profile$YammerMAU/usage_profile$YammerEnabledUsers
  usage_profile$OutlookMobileUsagePaidPerc <- usage_profile$OutlookMAU_Mobile/usage_profile$EXOPaidAvailableUnits
  usage_profile$OutlookMobileUsageEnabledPerc <- usage_profile$OutlookMAU_Mobile/usage_profile$EXOEnabledUsers
  usage_profile$ProPlusPerpetualUsagePaidPerc <- usage_profile$DesktopPerpetual/usage_profile$ProPlusPaidAvailableUnits
  usage_profile$ProPlusPerpetualUsageEnabledPerc <- usage_profile$DesktopPerpetual/usage_profile$PPDEnabledUsers
  
  usage_profile$IntuneUsagePaidPerc <- usage_profile$Intune_active_users/usage_profile$IntunePaidAvailableUnits
  usage_profile$IntuneUsageEnabledPerc <- usage_profile$Intune_active_users/usage_profile$Intune_configured_users
  usage_profile$AADPUsagePaidPerc <- usage_profile$AADP_active_users/usage_profile$AADPPaidAvailableUnits
  usage_profile$AADPUsageEnabledPerc <- usage_profile$AADP_active_users/usage_profile$AADP_configured_users
  usage_profile$AIPUsagePaidPerc <- usage_profile$AIP_active_users/usage_profile$AIPPaidAvailableUnits
  usage_profile$AIPUsageEnabledPerc <- usage_profile$AIP_active_users/usage_profile$AIP_configured_users
  usage_profile$OATPUsagePaidPerc <- usage_profile$OATP_active_users/usage_profile$OATPPaidAvailableUnits
  
  usage_profile[sapply(usage_profile, simplify = 'matrix', is.infinite)] <- 0 # Checked the distribution, mostly very small MAU like 1 till 90th perc. 
  usage_profile[ , 157:179 ][ usage_profile[ , 157:179  ] > 1 ] <- 1 
  
  #Geo - replicate for all the cohorts
  colnames(geo)[1] <- "FinalTPID"
  geo$FinalTPID <- as.numeric(geo$FinalTPID)
  geo <- geo %>% dplyr::select(FinalTPID, ym, GeoMapping, IndustryMapping)
  
  #licenses - Per workload/Total
  licenses$o365E3Perc <- (licenses$O365E3+ licenses$O365E3add)/licenses$TotalO365
  licenses$o365e5Perc <- (licenses$O365E5 + licenses$O365E5add)/licenses$TotalO365
  licenses$o365E12Perc <- (licenses$O365E12 + licenses$O365E12add)/licenses$TotalO365
  licenses$o365E4Perc <- (licenses$O365E4 + licenses$O365E4add)/licenses$TotalO365
  licenses$m365e3Perc <- licenses$M365E3/licenses$TotalM365
  licenses$m365e5perc <- licenses$M365E5/licenses$TotalM365
  licenses$m365f1perc <- licenses$M365F1/licenses$TotalM365
  licenses$TotalEMS <- licenses$EMSE3 + licenses$EMSE5 + 
    licenses$CloudAppSecurity + licenses$RightsMgmt + licenses$RemoteApp +
    licenses$MobIdentity + licenses$InfoProtPrem + licenses$AADP2 + licenses$AAD +
    licenses$Intune +  licenses$IntuneClient
  licenses$emse3perc <- (licenses$EMSE3)/licenses$TotalEMS
  licenses$emse5perc <- (licenses$EMSE5)/licenses$TotalEMS
  licenses$emsAddonsperc <- (licenses$CloudAppSecurity + licenses$RightsMgmt + licenses$RemoteApp +
                               licenses$MobIdentity + licenses$InfoProtPrem + licenses$AADP2 + licenses$AAD +
                               licenses$Intune +  licenses$IntuneClient)/licenses$TotalEMS
  
  
  licenses$EMSE3CaoTotalEMS <- licenses$EMSE3Cao/licenses$TotalEMS
  licenses$EMSE3FuslTotalEMS <- licenses$EMSE3Fusl/licenses$TotalEMS
  licenses$EMSE5FuslTotalEMS <- licenses$EMSE5Fusl/licenses$TotalEMS
  licenses$EMSE5CaoTotalEMS <- licenses$EMSE5Cao/licenses$TotalEMS
  
  
  licenses[sapply(licenses, simplify = 'matrix', is.infinite)] <- 0
  
  #revenue - keep features as is
  
  colnames(licenses)[1] <- "FinalTPID"
  colnames(revenue)[1] <- "FinalTPID"
  
  
  
  data <- usage_profile %>% left_join(geo,by = c("FinalTPID", "ym")) %>% 
    left_join(licenses,by = c("FinalTPID", "ym")) %>% 
    left_join(revenue,by = c("FinalTPID", "ym"))
  
  data <- labels %>% inner_join(data, by = c("FinalTPID", "ym"))
  
  data[sapply(data, simplify = 'matrix', is.infinite)] <- 0
  data[sapply(data, simplify = 'matrix', is.nan)] <- 0
  data[sapply(data, simplify = 'matrix', is.na)] <- 0
  
  data <- data %>% dplyr::select(-contains("ym1"))
  return(data)
}

# Training
labels <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\M365Labels6months.csv")
colnames(labels)[1] <- "FinalTPID"
labels$ym <- "FY19Q3"
tenant_profile <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\TenantProfile.csv")
tenant_profile <- tenant_profile %>% filter(ym == "FY19Q3")
collab <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\Collaboration.csv")
collab <- collab %>% filter(ym == "FY19Q3")
o365_usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\O365Usage.csv")
o365_usage <- o365_usage %>% filter(ym == "FY19Q3")
ems_usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\EMSUsage.csv")
ems_usage <- ems_usage %>% filter(ym == "FY19Q3")
geo_indus <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\GeoIndustry.csv")
geo_indus$ym <- "FY19Q3"
licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\Licenses.csv")
licenses <- licenses %>% filter(ym == "FY19Q3")
revenue <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\Revenue.csv") 
revenue <- revenue %>% filter(ym == "FY19Q3")

training_features <- aggregate(labels, tenant_profile, collab, o365_usage, ems_usage, geo_indus, licenses, revenue)
write.csv(training_features, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\AllFeaturesv3.csv", row.names = FALSE)

#Scoring
tenant_profile <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\TenantProfile.csv")
tenant_profile <- tenant_profile %>% filter(ym == "FY20Q1")
collab <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\Collaboration.csv")
collab <- collab %>% filter(ym == "FY20Q1")
o365_usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\O365Usage.csv")
o365_usage <- o365_usage %>% filter(ym == "FY20Q1")
ems_usage <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\EMSUsage.csv")
ems_usage <- ems_usage %>% filter(ym == "FY20Q1")
geo_indus <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\GeoIndustry.csv")
geo_indus$ym <- "FY20Q1"
licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\Licenses.csv") 
licenses <- licenses %>% filter(ym == "FY20Q1")
revenue <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\Revenue.csv") 
revenue <- revenue %>% filter(ym == "FY20Q1")

scoring_features <- aggregate(target_list, tenant_profile, collab, o365_usage, ems_usage, geo_indus, licenses, revenue)
write.csv(scoring_features, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\AllFeaturesv3.csv", row.names = FALSE)


