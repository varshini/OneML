library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)
options(stringsAsFactors = FALSE)

# O365 Usage #
o365 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-05_O365MauTimeseries.csv")
o365_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-06_MauByWorkload.csv")
colnames(o365_june)[3] <- "Workload" 
o365 <- rbind(o365, o365_june)

proplus <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-05_ProPlusMauTimeseries.csv")
proplus_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-06_ProPlus.csv")
proplus <- rbind(proplus, proplus_june)
proplus <- gather(proplus, Workload, Mau, ProPlusMAU:DesktopMobileMAU)

teams <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-05_TeamsMauTimeseries.csv")
teams_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-06_Teams.csv")
teams <- rbind(teams, teams_june)
teams <- gather(teams, Workload, Mau, TeamsMau:TeamsMobileMAU)

teams_platform <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-05_TeamsPlatformMauTimeseries.csv")
teamsp_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\O365Usage\\2020-06_TeamsPlatform.csv")
teams_platform <- rbind(teams_platform, teamsp_june)
colnames(teams_platform)[3] <- "Workload"

o365_usage <- rbind(o365, proplus, teams, teams_platform)
o365_usage$ym <- format(as.Date(o365_usage$Date, "%m/%d/%Y"), "%Y%m")
o365_usage <- spread(o365_usage, Workload, Mau)
o365_usage[is.na(o365_usage)] <- 0

# EMS Usage #
ems <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\EMSUsage\\2020-05_EMSMauTimeseries.csv")
ems_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\EMSUsage\\2020-06_MauByWorkload.csv")
colnames(ems_june)[3] <- "Workload"
ems <- rbind(ems, ems_june)

aadp <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\EMSUsage\\2020-05_AADPMauTimeseries.csv")
aadp_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\EMSUsage\\2020-06_AADPUsage.csv")
aadp <- rbind(aadp, aadp_june)
aadp <- gather(aadp, Workload, Mau, AADPUsageCount:PIMActiveUniqueUserCount)

intune <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\EMSUsage\\2020-05_IntuneMauTimeseries.csv")
intune_june <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\EMSUsage\\2020-06_IntuneUsage.csv")
intune <- rbind(intune, intune_june)
intune$Workload <- "Intune"
colnames(intune)[3] <- "Mau"
intune <- intune %>% select(Date, FinalTPID, Workload, Mau)

ems_usage <- rbind(ems, aadp, intune)
ems_usage$ym <- format(as.Date(ems_usage$Date, "%m/%d/%Y"), "%Y%m")
ems_usage <- spread(ems_usage, Workload, Mau)
ems_usage[is.na(ems_usage)] <- 0

all_usage <- full_join(o365_usage, ems_usage , by = c("FinalTPID", "ym"))

Trending <- function(x){
  n <- length(x)
  x[ x == 0 ] <- 1
  if(n<6 || sum(x>0)<6 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}

featurizeUsage <- function(cohort_data, cohort_ym) 
{

  cohort_data <- cohort_data %>% filter(ym <= cohort_ym)
  features <- cohort_data %>% 
    group_by(FinalTPID) %>% 
    arrange(ym) %>% 
    mutate(ESLTYOfficeClientTrend = Trending(`ESLTYOfficeClient`),
           EXOTrend = Trending(`EXO`),
           ODBTrend = Trending(`ODB`),
           SPOTrend = Trending(`SPO`),
           ProPlusMAUTrend = Trending(`ProPlusMAU`),
           DesktopProPlusMAUTrend = Trending(`DesktopProPlusMAU`),
           DesktopPerpetualMAUTrend = Trending(`DesktopPerpetualMAU`),
           DesktopSubscriptionMAUTrend = Trending(`DesktopSubscriptionMAU`),
           DesktopWebMAUTrend = Trending(`DesktopWebMAU`),
           DesktopMobileMAUTrend = Trending(`DesktopMobileMAU`),
           TeamsMauTrend = Trending(`TeamsMau`),
           TeamsDesktopMAUTrend = Trending(`TeamsDesktopMAU`),
           TeamsMobileMAUTrend = Trending(`TeamsMobileMAU`),
           SecondPartyTrend = Trending(`2nd Party`),
           ThirdPartyTrend = Trending(`3rd Party`),
           LOBTrend = Trending(`LOB`),
           SideloadedAppThirdPartyTrend = Trending(`SideloadedApp - 3rd Party`),
           SideloadedAppLOBTrend = Trending(`SideloadedApp - LOB`),
           SideloadedAppUnknownTrend = Trending(`SideloadedApp - Unknown`),
           MCASTrend = Trending(MCAS),
           MIGTrend = Trending(MIG),
           MIPTrend = Trending(MIP),
           OATPTrend = Trending(OATP),
           AADPUsageCountTrend = Trending(AADPUsageCount),
           CAActiveUniqueUserCountTrend = Trending(CAActiveUniqueUserCount),
           ThirdPartyActiveUniqueUserCountTrend = Trending(ThirdPartyActiveUniqueUserCount),
           B2BActiveUniqueUserCountTrend = Trending(B2BActiveUniqueUserCount),
           PIMActiveUniqueUserCountTrend = Trending(PIMActiveUniqueUserCount),
           IntuneMADTrend = Trending(Intune),
           ESLTYOfficeClientMean = mean(`ESLTYOfficeClient`),
           EXOMean = mean(`EXO`),
           ODBMean = mean(`ODB`),
           SPOMean = mean(`SPO`),
           ProPlusMAUMean = mean(`ProPlusMAU`),
           DesktopProPlusMAUMean = mean(`DesktopProPlusMAU`),
           DesktopPerpetualMAUMean = mean(`DesktopPerpetualMAU`),
           DesktopSubscriptionMAUMean = mean(`DesktopSubscriptionMAU`),
           DesktopWebMAUMean = mean(`DesktopWebMAU`),
           DesktopMobileMAUMean = mean(`DesktopMobileMAU`),
           TeamsMauMean = mean(`TeamsMau`),
           TeamsDesktopMAUMean = mean(`TeamsDesktopMAU`),
           TeamsMobileMAUMean = mean(`TeamsMobileMAU`),
           SecondPartyMean = mean(`2nd Party`),
           ThirdPartyMean = mean(`3rd Party`),
           LOBMean = mean(`LOB`),
           SideloadedAppThirdPartyMean = mean(`SideloadedApp - 3rd Party`),
           SideloadedAppLOBMean = mean(`SideloadedApp - LOB`),
           SideloadedAppUnknownMean = mean(`SideloadedApp - Unknown`),
           MCASMean = mean(MCAS),
           MIGMean = mean(MIG),
           MIPMean = mean(MIP),
           OATPMean = mean(OATP),
           AADPUsageCountMean = mean(AADPUsageCount),
           CAActiveUniqueUserCountMean = mean(CAActiveUniqueUserCount),
           ThirdPartyActiveUniqueUserCountMean = mean(ThirdPartyActiveUniqueUserCount),
           B2BActiveUniqueUserCountMean = mean(B2BActiveUniqueUserCount),
           PIMActiveUniqueUserCountMean = mean(PIMActiveUniqueUserCount),
           IntuneMADMean = mean(Intune)
           )
  features <- features %>% filter(ym == cohort_ym)
  features <- features %>% rename(ESLTYOfficeClient = `ESLTYOfficeClient`,
                                  EXO = `EXO`,
                                  ODB = `ODB`,
                                  SPO = `SPO`,
                                  ProPlusMAU = `ProPlusMAU`,
                                  DesktopProPlusMAU = `DesktopProPlusMAU`,
                                  DesktopPerpetualMAU = `DesktopPerpetualMAU`,
                                  DesktopSubscriptionMAU = `DesktopSubscriptionMAU`,
                                  DesktopWebMAU = `DesktopWebMAU`,
                                  DesktopMobileMAU = `DesktopMobileMAU`,
                                  TeamsMau = `TeamsMau`,
                                  TeamsDesktopMAU = `TeamsDesktopMAU`,
                                  TeamsMobileMAU = `TeamsMobileMAU`,
                                  SecondParty = `2nd Party`,
                                  ThirdParty = `3rd Party`,
                                  LOB = `LOB`,
                                  SideloadedAppThirdParty = `SideloadedApp - 3rd Party`,
                                  SideloadedAppLOB = `SideloadedApp - LOB`,
                                  SideloadedAppUnknown = `SideloadedApp - Unknown`,
                                  MCAS = MCAS,
                                  MIG = MIG,
                                  MIP = MIP,
                                  OATP = OATP,
                                  AADPUsageCount = AADPUsageCount,
                                  CAActiveUniqueUserCount = CAActiveUniqueUserCount,
                                  ThirdPartyActiveUniqueUserCount = ThirdPartyActiveUniqueUserCount,
                                  B2BActiveUniqueUserCount = B2BActiveUniqueUserCount,
                                  PIMActiveUniqueUserCount = PIMActiveUniqueUserCount,
                                  IntuneMAD = Intune) 
  
  return (features)
}


####### TRAINING PERIOD #########

usage_training <- featurizeUsage(all_usage, "201912") #Trends, Mean, Actual Snapshots, Ratios
usage_training <-  subset(usage_training, select = -c(Date.x,V1, Date.y) )
usage_training[is.na(usage_training)] <- 0 
usage_training <- rapply(usage_training, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
usage_training <- rapply(usage_training, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(usage_training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Training_Usage.csv", row.names = FALSE)

####### SCORING PERIOD #########

usage_scoring <- featurizeUsage(all_usage, "202006") #Trends, Mean, Actual Snapshots, Ratios
usage_scoring <-  subset(usage_scoring, select = -c(Date.x,V1, Date.y) )
usage_scoring[is.na(usage_scoring)] <- 0 
usage_scoring <- rapply(usage_scoring, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
usage_scoring <- rapply(usage_scoring, f=function(x) ifelse(is.infinite(x),0,x), how="replace" )
write.csv(usage_scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\Usage\\Scoring_Usage.csv", row.names = FALSE)


