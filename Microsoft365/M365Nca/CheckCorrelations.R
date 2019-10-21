# Analysis on insights about what drives upsell based on the chosen significant model features # 

library(dplyr)

training_features <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\Features\\Training\\AllFeaturesv3.csv")


## Usage features ##

data <- training_features
feature_name <- "HasEMSSku"
data <- data %>% group_by(HasEMSSku) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "HasWindowsSku"
data <- data %>% group_by(HasWindowsSku) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "HasM365SKUE3"
data <- data %>% group_by(HasM365SKUE3) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

# feature 1
data <- training_features
feature_name <- "IntuneUsagePaidPerc"
data$IntuneUsagePaidPerc <- data$IntuneUsagePaidPerc * 100
data$buckets <- case_when(data$IntuneUsagePaidPerc == 0 ~ "0", 
                          data$IntuneUsagePaidPerc > 0 & data$IntuneUsagePaidPerc <= 10 ~ "1-10",
                          data$IntuneUsagePaidPerc > 10 & data$IntuneUsagePaidPerc <= 30 ~ "10-30",
                          data$IntuneUsagePaidPerc > 30 & data$IntuneUsagePaidPerc <= 50 ~ "30-50",
                          data$IntuneUsagePaidPerc > 50 ~ "50+")

data$buckets <- case_when(data$IntuneUsagePaidPerc == 0 ~ "0", 
                          data$IntuneUsagePaidPerc > 0 & data$IntuneUsagePaidPerc <= 10 ~ "1-10",
                          data$IntuneUsagePaidPerc > 10 & data$IntuneUsagePaidPerc <= 30 ~ "10-30",
                          data$IntuneUsagePaidPerc > 30 ~ "30+")

data <- training_features
feature_name <- "IntuneUsageEnabledPerc"
data$IntuneUsageEnabledPerc <- data$IntuneUsageEnabledPerc * 100
data$buckets <- case_when(data$IntuneUsageEnabledPerc == 0 ~ "0", 
                          data$IntuneUsageEnabledPerc > 0 & data$IntuneUsageEnabledPerc <= 10 ~ "1-10",
                          data$IntuneUsageEnabledPerc > 10 & data$IntuneUsageEnabledPerc <= 30 ~ "10-30",
                          data$IntuneUsageEnabledPerc > 30 ~ "30+")

data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "IntuneEnabledPerc" # not using this
data$IntuneEnabledPerc <- data$IntuneEnabledPerc * 100
data$buckets <- case_when(data$IntuneEnabledPerc == 0 ~ "0", 
                          data$IntuneEnabledPerc > 0 & data$IntuneEnabledPerc <= 10 ~ "1-10",
                          data$IntuneEnabledPerc > 10 & data$IntuneEnabledPerc <= 30 ~ "10-30",
                          data$IntuneEnabledPerc > 30 & data$IntuneEnabledPerc <= 50 ~ "10-30"
                          data$IntuneEnabledPerc > 50 ~ "50+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

# feature 2
data <- training_features
feature_name <- "AIPUsageEnabledPerc"
data$AIPUsageEnabledPerc <- data$AIPUsageEnabledPerc * 100
data$buckets <- case_when(data$AIPUsageEnabledPerc == 0 ~ "0", 
                          data$AIPUsageEnabledPerc > 0 ~ "1")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

##### TODO #####
data <- training_features
feature_name <- "AADPUsageEnabledPerc"  
data$AADPUsageEnabledPerc <- data$AADPUsageEnabledPerc * 100
data$buckets <- case_when(data$AADPUsageEnabledPerc == 0 ~ "0", 
                          data$AADPUsageEnabledPerc > 0 & data$AADPUsageEnabledPerc <= 30 ~ "1-30",
                          data$AADPUsageEnabledPerc > 30 & data$AADPUsageEnabledPerc <= 70 ~ "30-70",
                          data$AADPUsageEnabledPerc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "OATPUsagePaidPerc"   
data$OATPUsagePaidPerc <- data$OATPUsagePaidPerc * 100
data$buckets <- case_when(data$OATPUsagePaidPerc == 0 ~ "0", 
                          data$OATPUsagePaidPerc > 0 & data$OATPUsagePaidPerc <= 30 ~ "1-30",
                          data$OATPUsagePaidPerc > 30 & data$OATPUsagePaidPerc <= 70 ~ "30-70",
                          data$OATPUsagePaidPerc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

# feature 3
data <- training_features
feature_name <- "ProPlusUsagePaidPerc"
data$ProPlusUsagePaidPerc <- data$ProPlusUsagePaidPerc * 100
data$buckets <- case_when(data$ProPlusUsagePaidPerc == 0 ~ "0", 
                          data$ProPlusUsagePaidPerc > 0 & data$ProPlusUsagePaidPerc <= 10 ~ "1-30",
                          data$ProPlusUsagePaidPerc > 10 & data$ProPlusUsagePaidPerc <= 30 ~ "10-30",
                          data$ProPlusUsagePaidPerc > 50 ~ "50+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

# feature 3
data <- training_features
feature_name <- "ProplusUsageEnabledPerc" # CHECK THE CALCULATION OF THE RATIO
data <- data %>% filter(ProPlusPaidAvailableUnits > 10)
data$ProplusUsageEnabledPerc <- data$ProplusUsageEnabledPerc * 100
data$buckets <- case_when(data$ProplusUsageEnabledPerc == 0 ~ "0", 
                          data$ProplusUsageEnabledPerc > 0 & data$ProplusUsageEnabledPerc <= 20 ~ "1-20",
                          data$ProplusUsageEnabledPerc > 20 & data$ProplusUsageEnabledPerc <= 40 ~ "20-40",
                          data$ProplusUsageEnabledPerc > 40 & data$ProplusUsageEnabledPerc <= 60 ~ "40-60",
                          data$ProplusUsageEnabledPerc > 60 ~ "60+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
data$DesktopSubscriptionEnabledPerc <- data$DesktopSubscription/data$PPDEnabledUsers
data$DesktopSubscriptionEnabledPerc[is.infinite(data$DesktopSubscriptionEnabledPerc)] <- 0
data$DesktopSubscriptionEnabledPerc[is.nan(data$DesktopSubscriptionEnabledPerc)] <- 0
feature_name <- "DesktopSubscriptionEnabledPerc" 
data$DesktopSubscriptionEnabledPerc <- data$DesktopSubscriptionEnabledPerc * 100
data$buckets <- case_when(data$DesktopSubscriptionEnabledPerc == 0 ~ "0", 
                          data$DesktopSubscriptionEnabledPerc > 0 & data$DesktopSubscriptionEnabledPerc <= 10 ~ "1-10",
                          data$DesktopSubscriptionEnabledPerc > 10 & data$DesktopSubscriptionEnabledPerc <= 30 ~ "10-30",
                          data$DesktopSubscriptionEnabledPerc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
data$DesktopSubscriptionPaidPerc <- data$DesktopSubscription/data$ProPlusPaidAvailableUnits
data$DesktopSubscriptionPaidPerc[is.infinite(data$DesktopSubscriptionPaidPerc)] <- 0
data$DesktopSubscriptionPaidPerc[is.nan(data$DesktopSubscriptionPaidPerc)] <- 0
feature_name <- "DesktopSubscriptionPaidPerc" 
data$DesktopSubscriptionEnabledPerc <- data$DesktopSubscriptionPaidPerc * 100
data$buckets <- case_when(data$DesktopSubscriptionPaidPerc == 0 ~ "0", 
                          data$DesktopSubscriptionPaidPerc > 0 & data$DesktopSubscriptionPaidPerc <= 10 ~ "1-10",
                          data$DesktopSubscriptionPaidPerc > 10 & data$DesktopSubscriptionPaidPerc <= 20 ~ "10-20",
                          data$DesktopSubscriptionPaidPerc > 20 & data$DesktopSubscriptionPaidPerc <= 40 ~ "20-40",
                          data$DesktopSubscriptionPaidPerc > 40 ~ "40+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "DesktopSubscription" 
data$buckets <- case_when(data$DesktopSubscription == 0 ~ "0", 
                          data$DesktopSubscription > 0 & data$DesktopSubscription <= 10 ~ "1-10",
                          data$DesktopSubscription > 10 & data$DesktopSubscription <= 20 ~ "10-20",
                          data$DesktopSubscription > 20 & data$DesktopSubscription <= 40 ~ "20-40",
                          data$DesktopSubscription > 40 ~ "40+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

# feature 4
data <- training_features
feature_name <- "MCASEnabledPerc"
data$MCASEnabledPerc <- data$MCASEnabledPerc * 100
data$buckets <- case_when(data$MCASEnabledPerc == 0 ~ "0", 
                          data$MCASEnabledPerc > 0 & data$MCASEnabledPerc <= 30 ~ "1-30",
                          data$MCASEnabledPerc > 30 & data$MCASEnabledPerc <= 70 ~ "30-70",
                          data$MCASEnabledPerc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


data <- training_features
feature_name <- "AADPP2EnabledPerc"
data$AADPP2EnabledPerc <- data$AADPP2EnabledPerc * 100
data$buckets <- case_when(data$AADPP2EnabledPerc == 0 ~ "0", 
                          data$AADPP2EnabledPerc > 0 & data$AADPP2EnabledPerc <= 30 ~ "1-30",
                          data$AADPP2EnabledPerc > 30 ~ "30+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "WDATPEnabledPerc"
data$WDATPEnabledPerc <- data$WDATPEnabledPerc * 100
data$buckets <- case_when(data$WDATPEnabledPerc == 0 ~ "0", 
                          data$WDATPEnabledPerc > 0 ~ "1")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

#feature 2
data <- training_features
feature_name <- "EMSE5EnabledUsers"
data$buckets <- case_when(data$EMSE5EnabledUsers == 0 ~ "0", 
                          data$EMSE5EnabledUsers > 0 & data$EMSE5EnabledUsers <= 100 ~ "1-100",
                          data$EMSE5EnabledUsers > 100 & data$EMSE5EnabledUsers <= 200 ~ "100-300",
                          data$EMSE5EnabledUsers > 200 ~ "200+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "Intune_configured_users"
data$buckets <- case_when(data$Intune_configured_users == 0 ~ "0", 
                          data$Intune_configured_users > 0 & data$Intune_configured_users <= 100 ~ "1-100",
                          data$Intune_configured_users > 100 & data$Intune_configured_users <= 200 ~ "100-200",
                          data$Intune_configured_users > 200 ~ "200+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "Intune_active_users"
data$buckets <- case_when(data$Intune_active_users == 0 ~ "0", 
                          data$Intune_active_users > 0 & data$Intune_active_users <= 100 ~ "1-100",
                          data$Intune_active_users > 100 & data$Intune_active_users <= 200 ~ "100-200",
                          data$Intune_active_users > 200 ~ "200+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


## License Features ##
#feature 1
data <- training_features
feature_name <- "o365E4Perc"
data$o365E4Perc <- data$o365E4Perc * 100
data$buckets <- case_when(data$o365E4Perc == 0 ~ "0", 
                          data$o365E4Perc > 0 & data$o365E4Perc <= 40 ~ "1-40",
                          data$o365E4Perc > 40 ~ "40+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

#feature 2
data <- training_features
feature_name <- "o365E3Perc"
data$o365E3Perc <- data$o365E3Perc * 100
data$buckets <- case_when(data$o365E3Perc == 0 ~ "0", 
                          data$o365E3Perc > 0 & data$o365E3Perc <= 30 ~ "1-30",
                          data$o365E3Perc > 30 & data$o365E3Perc <= 70 ~ "30-70",
                          data$o365E3Perc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

#feature 2
data <- training_features
feature_name <- "O365E3add"
data$buckets <- case_when(data$O365E3add == 0 ~ "0", 
                          data$O365E3add > 0 & data$O365E3add <= 100 ~ "1-100",
                          data$O365E3add > 100 & data$O365E3add <= 300 ~ "100-300",
                          data$O365E3add > 300 ~ "300+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

#feature 3
data <- training_features
feature_name <- "o365E12Perc" # not using
data$o365E12Perc <- data$o365E12Perc * 100
data$buckets <- case_when(data$o365E12Perc == 0 ~ "0", 
                          data$o365E12Perc > 0 & data$o365E12Perc <= 30 ~ "1-30",
                          data$o365E12Perc > 30 & data$o365E12Perc <= 70 ~ "30-70",
                          data$o365E12Perc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


#feature 4 
data <- training_features
feature_name <- "emse3perc"
data$emse3perc <- data$emse3perc * 100
data$buckets <- case_when(data$emse3perc == 0 ~ "0", 
                          data$emse3perc > 0 & data$emse3perc <= 30 ~ "1-30",
                          data$emse3perc > 30 & data$emse3perc <= 70 ~ "30-70",
                          data$emse3perc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


# feature 5 
data <- training_features
feature_name <- "o365e5Perc" # not using
data$o365e5Perc <- data$o365e5Perc * 100
data$buckets <- case_when(data$o365e5Perc == 0 ~ "0", 
                          data$o365e5Perc > 0 & data$o365e5Perc <= 10 ~ "1-10",
                          data$o365e5Perc > 10 & data$o365e5Perc <= 30 ~ "10-30",
                          data$o365e5Perc > 30 & data$o365e5Perc <= 50 ~ "30-50",
                          data$o365e5Perc > 50 ~ "50+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


data <- training_features
feature_name <- "EMSE3CaoTotalEMS" # not using
data$EMSE3CaoTotalEMS <- data$EMSE3CaoTotalEMS * 100
data$buckets <- case_when(data$EMSE3CaoTotalEMS == 0 ~ "0", 
                          data$EMSE3CaoTotalEMS > 0 & data$EMSE3CaoTotalEMS <= 30 ~ "1-30",
                          data$EMSE3CaoTotalEMS > 30 & data$EMSE3CaoTotalEMS <= 70 ~ "30-70",
                          data$EMSE3CaoTotalEMS > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "EMSE3FuslTotalEMS" # not using
data$EMSE3FuslTotalEMS <- data$EMSE3FuslTotalEMS * 100
data$buckets <- case_when(data$EMSE3FuslTotalEMS == 0 ~ "0", 
                          data$EMSE3FuslTotalEMS > 0 & data$EMSE3FuslTotalEMS <= 30 ~ "1-30",
                          data$EMSE3FuslTotalEMS > 30 & data$EMSE3FuslTotalEMS <= 70 ~ "30-70",
                          data$EMSE3FuslTotalEMS > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


data <- training_features
feature_name <- "WindowsPaidTotalPerc"
data$WindowsPaidTotalPerc <- data$WindowsPaidTotalPerc * 100
data$buckets <- case_when(data$WindowsPaidTotalPerc == 0 ~ "0", 
                          data$WindowsPaidTotalPerc > 0 & data$WindowsPaidTotalPerc <= 30 ~ "1-30",
                          data$WindowsPaidTotalPerc > 30 & data$WindowsPaidTotalPerc <= 70 ~ "30-70",
                          data$WindowsPaidTotalPerc > 70 ~ "70+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

# M365 mean seats - feature 1
data <- training_features
feature_name <- "M365E3Mean"
data$buckets <- case_when(data$M365E3Mean == 0 ~ "0", 
                          data$M365E3Mean > 0 & data$M365E3Mean <= 50 ~ "1-50",
                          data$M365E3Mean > 50 & data$M365E3Mean <= 150 ~ "50-150",
                          data$M365E3Mean > 150 & data$M365E3Mean <= 300 ~ "150-300")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "O365E5Mean"
data$buckets <- case_when(data$O365E5Mean == 0 ~ "0", 
                          data$O365E5Mean > 0 & data$O365E5Mean <= 150 ~ "1-150",
                          data$O365E5Mean > 150 & data$O365E5Mean <= 300 ~ "150-300", 
                          data$O365E5Mean > 300 ~ "300+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "EMSE3"
data$buckets <- case_when(data$EMSE3 == 0 ~ "0", 
                          data$EMSE3 > 0 & data$EMSE3 <= 50 ~ "1-50",
                          data$EMSE3 > 50 & data$EMSE3 <= 150 ~ "50-150",
                          data$EMSE3 > 150 & data$EMSE3 <= 300 ~ "150-300")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


data <- training_features
feature_name <- "EMSPaidAvailableUnits"
data$buckets <- case_when(data$EMSPaidAvailableUnits == 0 ~ "0", 
                          data$EMSPaidAvailableUnits > 0 & data$EMSPaidAvailableUnits <= 50 ~ "1-50",
                          data$EMSPaidAvailableUnits > 50 & data$EMSPaidAvailableUnits <= 150 ~ "50-150",
                          data$EMSPaidAvailableUnits > 150 & data$EMSPaidAvailableUnits <= 300 ~ "150-300", 
                          data$EMSPaidAvailableUnits > 300 & data$EMSPaidAvailableUnits <= 500 ~ "300-500", 
                          data$EMSPaidAvailableUnits > 500 ~ "500+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


data <- training_features
feature_name <- "MCASPaidAvailableUnits"
data$buckets <- case_when(data$MCASPaidAvailableUnits == 0 ~ "0", 
                          data$MCASPaidAvailableUnits > 0 & data$MCASPaidAvailableUnits <= 50 ~ "1-50",
                          data$MCASPaidAvailableUnits > 50 & data$MCASPaidAvailableUnits <= 150 ~ "50-150",
                          data$MCASPaidAvailableUnits > 150 & data$MCASPaidAvailableUnits <= 300 ~ "150-300", 
                          data$MCASPaidAvailableUnits > 300 & data$MCASPaidAvailableUnits <= 500 ~ "300-500", 
                          data$MCASPaidAvailableUnits > 500 ~ "500+")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


data <- training_features
feature_name <- "O365E5SecurityAnalyticsMean" # not much of a difference 
data$buckets <- case_when(data$O365E5SecurityAnalyticsMean == 0 ~ "0", 
                          data$O365E5SecurityAnalyticsMean > 0 & data$O365E5SecurityAnalyticsMean <= 50 ~ "1-50",
                          data$O365E5SecurityAnalyticsMean > 50 & data$O365E5SecurityAnalyticsMean <= 150 ~ "50-150",
                          data$O365E5SecurityAnalyticsMean > 150 & data$O365E5SecurityAnalyticsMean <= 300 ~ "150-300")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

## Trends ## 

#Trends - 'O365E3Trend','O365E4Trend','WindowsCoreNonM365E3YoYChange', 'EnterpriseMobilityCoreNonM365YoYChange',
data <- training_features
feature_name <- "O365E4Trend" # e3 and e4 trends don't make that much of a difference 
data$buckets <- case_when(data$O365E4Trend == 0 ~ "0", 
                          data$O365E4Trend > 0 & data$O365E4Trend <= 0.03 ~ "Slow",
                          data$O365E4Trend > 0.03 & data$O365E4Trend <= 0.1 ~ "medium",
                          data$O365E4Trend > 0.2  ~ "fast")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "EnterpriseMobilityCoreNonM365YoYChange"
data$buckets <- case_when(data$EnterpriseMobilityCoreNonM365YoYChange == 0 ~ "0", 
                          data$EnterpriseMobilityCoreNonM365YoYChange > 0 & data$EnterpriseMobilityCoreNonM365YoYChange <= 0.5 ~ "Slow",
                          data$EnterpriseMobilityCoreNonM365YoYChange > 0.5 ~ "fast")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "WindowsCoreNonM365E3YoYChange"
data$buckets <- case_when(data$WindowsCoreNonM365E3YoYChange == 0 ~ "0", 
                          data$WindowsCoreNonM365E3YoYChange > 0 & data$WindowsCoreNonM365E3YoYChange <= 0.5 ~ "Slow",
                          data$WindowsCoreNonM365E3YoYChange > 0.5 ~ "fast")
data <- data %>% group_by(buckets) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


## Other features ##
data <- training_features
feature_name <- "IsFastTrackTenant"
data <- data %>% group_by(IsFastTrackTenant) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "GeoMapping"
data <- data %>% dplyr::select(FinalTPID, GeoMapping, CustomerAdd.x)
geo_indus1 <- geo_indus %>% dplyr::select(Area, GeoMapping)
geo_indus1 <- geo_indus1 %>% distinct(Area, GeoMapping)
data <- inner_join(data,geo_indus1, by = "GeoMapping" )
data <- data %>% group_by(Area) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust

data <- training_features
feature_name <- "IndustryMapping"
data <- data %>% dplyr::select(FinalTPID, IndustryMapping, CustomerAdd.x)
geo_indus1 <- geo_indus %>% dplyr::select(Industry, IndustryMapping)
geo_indus1 <- geo_indus1 %>% distinct(Industry, IndustryMapping)
data <- inner_join(data,geo_indus1, by = "IndustryMapping" )
data <- data %>% group_by(Industry) %>% summarise(n_cust = n(), n_acq = sum( CustomerAdd.x ))
data$perc <- data$n_acq/data$n_cust


# Other EMS features - TODO

#  'IntuneClientMean',
# 'MobIdentity',
# 'CloudAppSecurityMean', 





