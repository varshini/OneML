# Featurize License data - M365, O365, EMS, Windows (Pending - TODO), OnPrem # 
library(dplyr)
library(readxl)
library(tidyr)
library(data.table)
require(TTR)
require(quantmod)
require(caTools)

Trending <- function(x){
  n <- length(x)
  x[ x == 0 ] <- 1
  if(n<4 || sum(x>0)<4 || any(is.na(x))) return(rep(0, n))
  cts <- caTools::runmean(x, 3, align = 'right')
  ROC(cts, 1, type = "discrete")
}

# M365 Seats

featurizeM365Seats <- function(cohort_data, cohort_ym, fy)
{
  cohort_data <- cohort_data %>% filter(ym1 <= cohort_ym)
  features <- cohort_data %>% 
    group_by(TPID) %>% 
    arrange(ym1) %>% 
    mutate(M365E3Trend = Trending(M365E3),
           M365E3Mean = mean(M365E3),
           M365E5Trend = Trending(M365E5),
           M365E5Mean = mean(M365E5),
           M365F1Trend = Trending(M365F1),
           M365F1Mean = mean(M365F1),
           TotalM365Trend = Trending(TotalM365),
           TotalM365Mean = mean(TotalM365))
  
  features <- features %>% filter(ym1 == cohort_ym)
  features$ym <- fy
  features <- data.frame(features)
  features[is.na(features)] <- 0
  return (features)
}
  

m365 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\M365Seats.csv")
#m365[ m365 == 0 ] <- 1
training_cohort_q3 <- featurizeM365Seats(m365, "201809", "FY19Q3")
training_cohort_q4 <- featurizeM365Seats(m365, "201812", "FY19Q4")
training_cohort_m365 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeM365Seats(m365, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeM365Seats(m365, "201906", "FY20Q2")
scoring_cohort_m365 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

# O365 Seats 

featurizeO365Seats <- function(cohort_data, cohort_ym, fy)
{
  #calculate total O365
  cohort_data <- cohort_data %>% filter(ym1 <= cohort_ym)
  features <- cohort_data %>% 
    group_by(TPID) %>% 
    arrange(ym1) %>% 
    mutate(TotalO365Trend = Trending(TotalO365),
           TotalO365Mean = mean(TotalO365),
           O365E12Trend = Trending(O365E12),
           O365E12Mean = mean(O365E12),
           O365E12addTrend = Trending(O365E12add),
           O365E12addMean = mean(O365E12add),
           O365E3Trend = Trending(O365E3),
           O365E3Mean = mean(O365E3), 
           O365E3addTrend = Trending(O365E3add),
           O365E3addMean = mean(O365E3add), 
           O365E4Trend = Trending(O365E4),
           O365E4Mean = mean(O365E4),
           O365E4addTrend = Trending(O365E4add),
           O365E4addMean = mean(O365E4add),
           O365E5Trend = Trending(O365E5),
           O365E5Mean = mean(O365E5),
           O365E5addTrend = Trending(O365E5add),
           O365E5addMean = mean(O365E5add))

  features <- features %>% filter(ym1 == cohort_ym)
  features$ym <- fy
  return (features)
  
}

o365 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\O365Seats.csv")
o365$TotalO365 <- o365$O365E12 + o365$O365E12add + o365$O365E3 + o365$O365E3add + o365$O365E4 + o365$O365E4add + o365$O365E5 + o365$O365E5add
#o365[ o365 == 0 ] <- 1

training_cohort_q3 <- featurizeO365Seats(o365, "201809", "FY19Q3")
training_cohort_q4 <- featurizeO365Seats(o365, "201812", "FY19Q4")
training_cohort_o365 <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeO365Seats(o365, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeO365Seats(o365, "201906", "FY20Q2")
scoring_cohort_o365 <- rbind(scoring_cohort_q1, scoring_cohort_q2)

# EMS Seats 

featurizeEMSSeats <- function(cohort_data, cohort_ym, fy)
{
  #calculate total O365
  cohort_data <- cohort_data %>% filter(ym1 <= cohort_ym)
  features <- cohort_data %>% 
    group_by(TPID) %>% 
    arrange(ym1) %>% 
    mutate(EMSE5Trend = Trending(EMSE5),
           EMSE5Mean = mean(EMSE5), 
           EMSE3Trend = Trending(EMSE3),
           EMSE3Mean = mean(EMSE3),
           CloudAppSecurityTrend = Trending(CloudAppSecurity),
           CloudAppSecurityMean = mean(CloudAppSecurity),
           RightsMgmtTrend = Trending(RightsMgmt),
           RightsMgmtMean = mean(RightsMgmt),
           RemoteAppTrend = Trending(RemoteApp),
           RemoteAppMean = mean(RemoteApp),
           MobIdentityTrend = Trending(MobIdentity),
           MobIdentityMean = mean(MobIdentity),
           InfoProtPremTrend = Trending(InfoProtPrem),
           InfoProtPremMean = mean(InfoProtPrem),
           AADP2Trend = Trending(AADP2),
           AADP2Mean = mean(AADP2),
           AADTrend = Trending(AAD),
           AADMean = mean(AAD),
           IntuneTrend = Trending(Intune),
           IntuneMean = mean(Intune),
           IntuneClientTrend = Trending(IntuneClient),
           IntuneClientMean = mean(IntuneClient)
           )
  features <- features %>% filter(ym1 == cohort_ym)
  features$ym <- fy
  return (features)
}

ems <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\EMSSeats.csv")
ems$EMSE5 <- ems$EMSE5Cao + ems$EMSE5Fusl + ems$EMSE5KSuite
ems$EMSE3 <- ems$EMSE3Cao + ems$EMSE3Fusl + ems$EMSCoreKSuite
#ems[ ems == 0 ] <- 1

training_cohort_q3 <- featurizeEMSSeats(ems, "201809", "FY19Q3")
training_cohort_q4 <- featurizeEMSSeats(ems, "201812", "FY19Q4")
training_cohort_ems <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeEMSSeats(ems, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeEMSSeats(ems, "201906", "FY20Q2")
scoring_cohort_ems <- rbind(scoring_cohort_q1, scoring_cohort_q2)

# On Prem Seats 

featurizeOnPremSeats <- function(cohort_data, cohort_ym, fy)
{
  #calculate total O365
  cohort_data <- cohort_data %>% filter(ym1 <= cohort_ym)
  features <- cohort_data %>% 
    group_by(TPID) %>% 
    arrange(ym1) %>% 
    mutate(AnnuityTrend = Trending(Annuity),
           AnnuityMean = mean(Annuity), 
           NonAnnuityTrend = Trending(NonAnnuity),
           NonAnnuityMean = mean(NonAnnuity),
           DarkAnnuityTrend = Trending(DarkAnnuity),
           DarkAnnuityMean = mean(DarkAnnuity))
  features <- features %>% filter(ym1 == cohort_ym)
  features$ym <- fy
  return (features)
}

onprem <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\OnPremSeats.csv")
#onprem[ onprem == 0 ] <- 1

training_cohort_q3 <- featurizeOnPremSeats(onprem, "201809", "FY19Q3")
training_cohort_q4 <- featurizeOnPremSeats(onprem, "201812", "FY19Q4")
training_cohort_onprem <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeOnPremSeats(onprem, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeOnPremSeats(onprem, "201906", "FY20Q2")
scoring_cohort_onprem <- rbind(scoring_cohort_q1, scoring_cohort_q2)

# Windows Seats 

featurizeWindowsSeats <- function(cohort_data, cohort_ym, fy)
{
  #calculate total O365
  cohort_data <- cohort_data %>% filter(ym1 <= cohort_ym)
  features <- cohort_data %>% 
    group_by(TPID) %>% 
    arrange(ym1) %>% 
    # change the names here for windows features 
    mutate(Win10PerDeviceTrend = Trending(Win10PerDevice),
           Win10PerDeviceMean = mean(Win10PerDevice), 
           Win10PerUserTrend = Trending(Win10PerUser),
           Win10PerUserMean = mean(Win10PerUser),
           WinClientTrend = Trending(WinClient),
           WinClientMean = mean(WinClient),
           WinE5Trend = Trending(WinE5),
           WinE5Mean = mean(WinE5))
  features <- features %>% filter(ym1 == cohort_ym)
  features$ym <- fy
  return (features)
}

windows <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\WindowsSeats.csv")
#windows[ windows == 0 ] <- 1

training_cohort_q3 <- featurizeWindowsSeats(windows, "201809", "FY19Q3")
training_cohort_q4 <- featurizeWindowsSeats(windows, "201812", "FY19Q4")
training_cohort_windows <- rbind(training_cohort_q3, training_cohort_q4)
scoring_cohort_q1 <- featurizeWindowsSeats(windows, "201903", "FY20Q1")
scoring_cohort_q2 <- featurizeWindowsSeats(windows, "201906", "FY20Q2")
scoring_cohort_windows <- rbind(scoring_cohort_q1, scoring_cohort_q2)

# full join all the data and have one license features file

training_cohort_onprem$TPID <- as.numeric(training_cohort_onprem$TPID)
training_cohort_o365$TPID <- as.numeric(training_cohort_o365$TPID)
training_cohort_m365$TPID <- as.numeric(training_cohort_m365$TPID)
training_cohort_ems$TPID <- as.numeric(training_cohort_ems$TPID)
training_cohort_windows$TPID <- as.numeric(training_cohort_windows$TPID)
final_license_training <- full_join(training_cohort_onprem, training_cohort_o365, by = c("TPID", "ym")) %>% 
                          full_join(., training_cohort_m365, by = c("TPID", "ym")) %>% 
                          full_join(., training_cohort_ems, by = c("TPID", "ym")) %>% 
                          full_join(., training_cohort_windows, by = c("TPID", "ym"))
final_license_training[is.na(final_license_training)] <- 0

scoring_cohort_onprem$TPID <- as.numeric(scoring_cohort_onprem$TPID)
scoring_cohort_o365$TPID <- as.numeric(scoring_cohort_o365$TPID)
scoring_cohort_m365$TPID <- as.numeric(scoring_cohort_m365$TPID)
scoring_cohort_ems$TPID <- as.numeric(scoring_cohort_ems$TPID)
scoring_cohort_windows$TPID <- as.numeric(scoring_cohort_windows$TPID)
final_license_scoring <- full_join(scoring_cohort_onprem, scoring_cohort_o365, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_m365, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_ems, by = c("TPID", "ym")) %>% 
  full_join(., scoring_cohort_windows, by = c("TPID", "ym"))
final_license_scoring[is.na(final_license_scoring)] <- 0

write.csv(final_license_training, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\Licenses.csv", row.names = FALSE)
write.csv(final_license_scoring, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Scoring\\Licenses.csv", row.names = FALSE)
