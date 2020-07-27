# Insights # 

library(dplyr)
training_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\TrainingFeaturesFinalv2.csv")

## Usage Features ##
# feature 1
data <- training_data
feature_name <- "IntuneUsagePaidPerc" # tried for usage enabled
data$IntuneUsagePaidPerc <- data$IntuneUsagePaidPerc * 100
data$buckets <- case_when(data$IntuneUsagePaidPerc == 0 ~ "0", 
                          data$IntuneUsagePaidPerc > 0 & data$IntuneUsagePaidPerc <= 10 ~ "1-10",
                          data$IntuneUsagePaidPerc > 10 & data$IntuneUsagePaidPerc <= 20 ~ "10-20",
                          data$IntuneUsagePaidPerc > 20 ~ "20+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "ProPlusUsagePaidPerc"
data$ProPlusUsagePaidPerc <- data$ProPlusUsagePaidPerc * 100
data$buckets <- case_when(data$ProPlusUsagePaidPerc == 0 ~ "0", 
                          data$ProPlusUsagePaidPerc > 0 & data$ProPlusUsagePaidPerc <= 10 ~ "1-10",
                          data$ProPlusUsagePaidPerc > 10 & data$ProPlusUsagePaidPerc <= 20 ~ "10-20",
                          data$ProPlusUsagePaidPerc > 20 & data$ProPlusUsagePaidPerc <= 30 ~ "20-30",
                          data$ProPlusUsagePaidPerc > 30 & data$ProPlusUsagePaidPerc <= 40 ~ "30-40",
                          data$ProPlusUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "PerpetualProPlusUsagePaidPerc"
data$PerpetualProPlusUsagePaidPerc <- data$PerpetualProPlusUsagePaidPerc * 100
data$buckets <- case_when(data$PerpetualProPlusUsagePaidPerc == 0 ~ "0", 
                          data$PerpetualProPlusUsagePaidPerc > 0 & data$PerpetualProPlusUsagePaidPerc <= 10 ~ "1-10",
                          data$PerpetualProPlusUsagePaidPerc > 10 & data$PerpetualProPlusUsagePaidPerc <= 20 ~ "10-20",
                          data$PerpetualProPlusUsagePaidPerc > 20 & data$PerpetualProPlusUsagePaidPerc <= 30 ~ "20-30",
                          data$PerpetualProPlusUsagePaidPerc > 30 & data$PerpetualProPlusUsagePaidPerc <= 40 ~ "30-40",
                          data$PerpetualProPlusUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "AADPUsagePaidPerc"
data$AADPUsagePaidPerc <- data$AADPUsagePaidPerc * 100
data$buckets <- case_when(data$AADPUsagePaidPerc == 0 ~ "0", 
                          data$AADPUsagePaidPerc > 0 & data$AADPUsagePaidPerc <= 10 ~ "1-10",
                          data$AADPUsagePaidPerc > 10 & data$AADPUsagePaidPerc <= 20 ~ "10-20",
                          data$AADPUsagePaidPerc > 20 & data$AADPUsagePaidPerc <= 30 ~ "20-30",
                          data$AADPUsagePaidPerc > 30 & data$AADPUsagePaidPerc <= 40 ~ "30-40",
                          data$AADPUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust


data <- training_data
feature_name <- "TeamsAppSecondPartyUsagePaidPerc"
data$SecondParty <- data$TeamsAppSecondPartyUsagePaidPerc * 100
data$buckets <- case_when(data$TeamsAppSecondPartyUsagePaidPerc == 0 ~ "0", 
                          data$TeamsAppSecondPartyUsagePaidPerc > 0 & data$TeamsAppSecondPartyUsagePaidPerc <= 10 ~ "1-10",
                          data$TeamsAppSecondPartyUsagePaidPerc > 10 & data$TeamsAppSecondPartyUsagePaidPerc <= 20 ~ "10-20",
                          data$TeamsAppSecondPartyUsagePaidPerc > 20 & data$TeamsAppSecondPartyUsagePaidPerc <= 30 ~ "20-30",
                          data$TeamsAppSecondPartyUsagePaidPerc > 30 & data$TeamsAppSecondPartyUsagePaidPerc <= 40 ~ "30-40",
                          data$TeamsAppSecondPartyUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "TeamsAppThirdPartyUsagePaidPerc"
data$SecondParty <- data$TeamsAppThirdPartyUsagePaidPerc * 100
data$buckets <- case_when(data$TeamsAppThirdPartyUsagePaidPerc == 0 ~ "0", 
                          data$TeamsAppThirdPartyUsagePaidPerc > 0 & data$TeamsAppThirdPartyUsagePaidPerc <= 10 ~ "1-10",
                          data$TeamsAppThirdPartyUsagePaidPerc > 10 & data$TeamsAppThirdPartyUsagePaidPerc <= 20 ~ "10-20",
                          data$TeamsAppThirdPartyUsagePaidPerc > 20 & data$TeamsAppThirdPartyUsagePaidPerc <= 30 ~ "20-30",
                          data$TeamsAppThirdPartyUsagePaidPerc > 30 & data$TeamsAppThirdPartyUsagePaidPerc <= 40 ~ "30-40",
                          data$TeamsAppThirdPartyUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust



data <- training_data
feature_name <- "OATPUsagePaidPerc"
data$OATPUsagePaidPerc <- data$OATPUsagePaidPerc * 100
data$buckets <- case_when(data$OATPUsagePaidPerc == 0 ~ "0", 
                          data$OATPUsagePaidPerc > 0 & data$OATPUsagePaidPerc <= 10 ~ "1-10",
                          data$OATPUsagePaidPerc > 10 & data$OATPUsagePaidPerc <= 20 ~ "10-20",
                          data$OATPUsagePaidPerc > 20 & data$OATPUsagePaidPerc <= 30 ~ "20-30",
                          data$OATPUsagePaidPerc > 30 & data$OATPUsagePaidPerc <= 40 ~ "30-40",
                          data$OATPUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "EXOUsagePaidPerc"
data$EXOUsagePaidPerc <- data$EXOUsagePaidPerc * 100
data$buckets <- case_when(data$EXOUsagePaidPerc == 0 ~ "0", 
                          data$EXOUsagePaidPerc > 0 & data$EXOUsagePaidPerc <= 10 ~ "1-10",
                          data$EXOUsagePaidPerc > 10 & data$EXOUsagePaidPerc <= 20 ~ "10-20",
                          data$EXOUsagePaidPerc > 20 & data$EXOUsagePaidPerc <= 30 ~ "20-30",
                          data$EXOUsagePaidPerc > 30 & data$EXOUsagePaidPerc <= 40 ~ "30-40",
                          data$EXOUsagePaidPerc > 40 & data$EXOUsagePaidPerc <= 50 ~ "40-50",
                          data$EXOUsagePaidPerc > 50 & data$EXOUsagePaidPerc <= 60 ~ "50-60",
                          data$EXOUsagePaidPerc > 60 & data$EXOUsagePaidPerc <= 70 ~ "60-70",
                          data$EXOUsagePaidPerc > 70 ~ "70+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "SPOUsagePaidPerc"
data$SPOUsagePaidPerc <- data$SPOUsagePaidPerc * 100
data$buckets <- case_when(data$SPOUsagePaidPerc == 0 ~ "0", 
                          data$SPOUsagePaidPerc > 0 & data$SPOUsagePaidPerc <= 10 ~ "1-10",
                          data$SPOUsagePaidPerc > 10 & data$SPOUsagePaidPerc <= 20 ~ "10-20",
                          data$SPOUsagePaidPerc > 20 & data$SPOUsagePaidPerc <= 30 ~ "20-30",
                          data$SPOUsagePaidPerc > 30 & data$SPOUsagePaidPerc <= 40 ~ "30-40",
                          data$SPOUsagePaidPerc > 40 ~ "40+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

#O2016_rl28_users
#SideloadedAppUnknown


## License Features ##
data <- training_data
feature_name <- "HasM365SKUE5"
data <- data %>% group_by(HasM365SKUE5) %>% summarise(n_cust = n(), n_acq = sum( M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "o365E3Perc"
data$o365E3Perc <- data$o365E3Perc * 100
data$buckets <- case_when(data$o365E3Perc == 0 ~ "0", 
                          data$o365E3Perc > 0 & data$o365E3Perc <= 10 ~ "1-10",
                          data$o365E3Perc > 10 & data$o365E3Perc <= 30 ~ "10-30",
                          data$o365E3Perc > 30 & data$o365E3Perc <= 60 ~ "30-60",
                          data$o365E3Perc > 60 ~ "60+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "o365E5Perc"
data$o365e5Perc <- data$o365e5Perc * 100
data$buckets <- case_when(data$o365e5Perc == 0 ~ "0", 
                          data$o365e5Perc > 0 & data$o365e5Perc <= 10 ~ "1-10",
                          data$o365e5Perc > 10 ~ "10+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "AnnuityPercent"
data$AnnuityPercent <- data$AnnuityPercent * 100
data$buckets <- case_when(data$AnnuityPercent == 0 ~ "0", 
                          data$AnnuityPercent > 0 & data$AnnuityPercent <= 10 ~ "1-10",
                          data$AnnuityPercent > 10 & data$AnnuityPercent <= 30 ~ "10-30",
                          data$AnnuityPercent > 30 & data$AnnuityPercent <= 60 ~ "30-60",
                          data$AnnuityPercent > 60 & data$AnnuityPercent <= 80 ~ "60-80",
                          data$AnnuityPercent > 80 ~ "80+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
data$PowerBI <- ifelse(data$PowerBIOfficeSuites > 0 | data$PowerBIPremium > 0 | data$PowerBIStandalonePro > 0, 1, 0)
data <- data %>% group_by(PowerBI) %>% summarise(n_cust = n(), n_acq = sum( M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "HasEMSSku"
data <- data %>% group_by(HasEMSSku) %>% summarise(n_cust = n(), n_acq = sum( M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "emse3perc"
data$emse3perc <- data$emse3perc * 100
data$buckets <- case_when(data$emse3perc == 0 ~ "0", 
                          data$emse3perc > 0 & data$emse3perc <= 10 ~ "1-10",
                          data$emse3perc > 10 & data$emse3perc <= 30 ~ "10-30",
                          data$emse3perc > 30 & data$emse3perc <= 60 ~ "30-60",
                          data$emse3perc > 60 ~ "60+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
feature_name <- "EMSE5TotalE5"
data$EMSE5TotalE5 <- data$EMSE5TotalE5 * 100
data$buckets <- case_when(data$EMSE5TotalE5 == 0 ~ "0", 
                          data$EMSE5TotalE5 > 0 & data$EMSE5TotalE5 <= 10 ~ "1-10",
                          data$EMSE5TotalE5 > 10 & data$EMSE5TotalE5 <= 30 ~ "10-30",
                          data$EMSE5TotalE5 > 30 & data$EMSE5TotalE5 <= 60 ~ "30-60",
                          data$EMSE5TotalE5 > 60 ~ "60+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum(M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
data$ATPFlag <- ifelse(data$AdvancedThreatProtectionPlan1 > 0 , 1, 0)
data <- data %>% group_by(ATPFlag) %>% summarise(n_cust = n(), n_acq = sum( M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
data <- data %>% group_by(HasWebex) %>% summarise(n_cust = n(), n_acq = sum( M365Label ))
data$perc <- data$n_acq/data$n_cust

data <- training_data
data <- data %>% group_by(IsFastTrackTenant) %>% summarise(n_cust = n(), n_acq = sum( M365Label ))
data$perc <- data$n_acq/data$n_cust

###########################################################################################################

aug2019 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Analysis\\2019-08_TeamsPlatformAppName.csv")
sep2019 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Analysis\\2019-09_TeamsPlatformAppName.csv")
oct2019 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Analysis\\2019-10_TeamsPlatformAppName.csv")
nov2019 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Analysis\\2019-11_TeamsPlatformAppName.csv")
dec2019 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Analysis\\2019-12_TeamsPlatformAppName.csv")


platform_data <- rbind(aug2019, sep2019, oct2019, nov2019, dec2019)

# Last 6 months 
platform_data <- platform_data %>% group_by(FinalTPID, AppType, AppName) %>% summarise(Mau = sum(Mau))



# Comparing with Rescube - M365

licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\AllLicensesByRevSumCat.csv")
licenses <- left_join(smctpids, licenses, by = "FinalTPID")
licenses[is.na(licenses)] <- 0

licenses$ym <- format(as.Date(licenses$Date, "%m/%d/%Y"), "%Y%m")
licenses <- licenses %>% select(FinalTPID, ym, Rev_Sum_Category, SeatCount)
licenses_wide <- licenses %>% spread(Rev_Sum_Category, SeatCount)
licenses_wide[is.na(licenses_wide)] <- 0

# Sum of license skus
licenses_wide$M365E3 <-  licenses_wide$`O365 - M365 E3 CAO` + licenses_wide$`O365 - M365 E3 FUSL`
licenses_wide$M365E5 <- licenses_wide$`O365 E5 - M365 E5 CAO` + licenses_wide$`O365 E5 - M365 E5 FUSL`
licenses_wide$TotalM365 <- licenses_wide$M365E3 + licenses_wide$M365E5 + licenses_wide$`O365 - M365 F1`

a <- filter(licenses_wide, ym == "202005")
a <- left_join(smctpids, a, by = "FinalTPID")
a[is.na(a)] <- 0
a <- a %>% select(FinalTPID, `O365 - M365 F1`, M365E3, M365E5, TotalM365)

# rescube_pull <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\ResCubePull.xlsx", sheet = "Sheet1") #excludes SMB according to FY20
# rescube_pull$FinalTPID <- as.integer(rescube_pull$FinalTPID)
# b <- left_join(smctpids, rescube_pull,by = "FinalTPID")
# b[is.na(b)] <- 0

rescube_pull <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\ResCubePull.xlsx", sheet = "Sheet3") # includes SMB according to fy20
rescube_pull$FinalTPID <- as.integer(rescube_pull$FinalTPID)
b1 <- left_join(smctpids, rescube_pull,by = "FinalTPID")
b1[is.na(b1)] <- 0
sum(b1$M365Seats)




c <- full_join(a, b1)
c[is.na(c)] <- 0
c$delta <- c$TotalM365 - c$M365Seats
  





# Comparing with Rescube - O365
licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\AllLicensesByRevSumCat.csv")
licenses <- left_join(smctpids, licenses, by = "FinalTPID")
licenses[is.na(licenses)] <- 0

licenses$ym <- format(as.Date(licenses$Date, "%m/%d/%Y"), "%Y%m")
licenses <- licenses %>% select(FinalTPID, ym, Rev_Sum_Category, SeatCount)
licenses_wide <- licenses %>% spread(Rev_Sum_Category, SeatCount)
licenses_wide[is.na(licenses_wide)] <- 0
licenses_wide$TotalO365 <- licenses_wide$`O365 Plan E1/E2` + licenses_wide$`O365 Plan E1/E2 Cloud Add-On` + 
  licenses_wide$`O365 Plan E3` + licenses_wide$`O365 Plan E3 Cloud Add-On` + licenses_wide$`O365 Plan E4` +
  licenses_wide$`O365 Plan E4 Cloud Add-On` +  licenses_wide$`O365 Plan E5` + licenses_wide$`O365 Plan E5 Cloud Add-On` 
a <- filter(licenses_wide, ym == "202005")
a <- a %>% select(FinalTPID, TotalO365)

rescube_pull <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\ResCubePull.xlsx", sheet = "Sheet2")
rescube_pull$FinalTPID <- as.integer(rescube_pull$FinalTPID)
b <- left_join(smctpids, rescube_pull,by = "FinalTPID")
b[is.na(b)] <- 0

c <- full_join(a, b)
c[is.na(c)] <- 0
