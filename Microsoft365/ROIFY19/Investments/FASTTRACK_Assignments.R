library(readxl)
library(lubridate)

# People touch - FRP + CSS

fy19_heavy <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\FTHeavy_FY19.csv")
colnames(fy19_heavy)[1] <- "TenantId"
fy19_heavy$TenantId <- tolower(fy19_heavy$TenantId)
fy19_heavy$DashboardCreationDate <- mdy(fy19_heavy$DashboardCreationDate)

fy20_heavy <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\FTHeavy_FY20.csv")
colnames(fy20_heavy)[1] <- "TenantId"
fy20_heavy$TenantId <- tolower(fy20_heavy$TenantId)
fy20_heavy$DashboardCreationDate <- mdy(fy20_heavy$DashboardCreationDate)

all_ft_heavy <- rbind(fy19_heavy, fy20_heavy)


## to calculate the elapsed months
elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

calculateMonths <- function(data, end_date)
{
  data <- filter(data, data$DashboardCreationDate <= end_date)
  data$NoOfMonths <- elapsed_months(as.Date(end_date), data$DashboardCreationDate)
  data$NoOfMonths <- ifelse(data$NoOfMonths > 0, data$NoOfMonths, 0)
  data$NoOfMonths <- ifelse(data$NoOfMonths > 10,10, data$NoOfMonths)
  data <- data %>% group_by(TenantId) %>% summarise(FTHeavyNoOfMonths = max(NoOfMonths))
  return (data)
}

jul1819 <- calculateMonths(all_ft_heavy,"2019-05-31")
jul1819$ym <- "201907"
aug1819 <- calculateMonths(all_ft_heavy,"2019-06-30")
aug1819$ym <- "201908"
sep1819 <- calculateMonths(all_ft_heavy,"2019-07-31")
sep1819$ym <- "201909"
oct1819 <- calculateMonths(all_ft_heavy,"2019-08-31")
oct1819$ym <- "201910"
nov1819 <- calculateMonths(all_ft_heavy,"2019-09-30")
nov1819$ym <- "201911"
dec1819 <- calculateMonths(all_ft_heavy,"2019-10-31")
dec1819$ym <- "201912"

ftheavy <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819) # Final df for heavy touch
write.csv(ftheavy, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\FTAssignment.csv", row.names = FALSE)

####### Automation #########
ftauto <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\FTAutomation.csv")
colnames(ftauto)[1] <- "TenantId"
ftauto$TenantId <- tolower(ftauto$TenantId)
ftauto$TenantCreatedDate <- mdy(ftauto$TenantCreatedDate)
ftauto <- filter(ftauto, !(ServiceName %in% c("Windows", "SfB(Skype for Business)", "StaffHub", "Yammer", "Microsoft Defender ATP (FY20)")))

calculateAutoMonths <- function(data, end_date)
{
  data <- filter(data, data$TenantCreatedDate <= end_date)
  data$NoOfMonths <- elapsed_months(as.Date(end_date), data$TenantCreatedDate)
  data$NoOfMonths <- ifelse(data$NoOfMonths > 0, data$NoOfMonths, 0)
  data$NoOfMonths <- ifelse(data$NoOfMonths > 10,10, data$NoOfMonths)
  data <- data %>% group_by(TenantId) %>% summarise(FTAutomationNoOfMonths = max(NoOfMonths))
  return (data)
}

# All workloads 
jul1819 <- calculateAutoMonths(ftauto,"2019-05-31")
jul1819$ym <- "201907"
aug1819 <- calculateAutoMonths(ftauto,"2019-06-30")
aug1819$ym <- "201908"
sep1819 <- calculateAutoMonths(ftauto,"2019-07-31")
sep1819$ym <- "201909"
oct1819 <- calculateAutoMonths(ftauto,"2019-08-31")
oct1819$ym <- "201910"
nov1819 <- calculateAutoMonths(ftauto,"2019-09-30")
nov1819$ym <- "201911"
dec1819 <- calculateAutoMonths(ftauto,"2019-10-31")
dec1819$ym <- "201912"

ftauto_all <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819) 
write.csv(ftauto_all, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FTAutomation.csv", row.names = FALSE)

# CoreWorkloads
ftautocore <- filter(ftauto, !ServiceName %in% c("Teams"))

jul1819 <- calculateAutoMonths(ftautocore,"2019-05-31")
jul1819$ym <- "201907"
aug1819 <- calculateAutoMonths(ftautocore,"2019-06-30")
aug1819$ym <- "201908"
sep1819 <- calculateAutoMonths(ftautocore,"2019-07-31")
sep1819$ym <- "201909"
oct1819 <- calculateAutoMonths(ftautocore,"2019-08-31")
oct1819$ym <- "201910"
nov1819 <- calculateAutoMonths(ftautocore,"2019-09-30")
nov1819$ym <- "201911"
dec1819 <- calculateAutoMonths(ftautocore,"2019-10-31")
dec1819$ym <- "201912"

ftauto_core <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819) 
write.csv(ftauto_core, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FTAutomation.csv", row.names = FALSE)

# ModernComms
ftautocomms <- filter(ftauto, ServiceName %in% c("TenantLevel", "Teams"))

jul1819 <- calculateAutoMonths(ftautocomms,"2019-05-31")
jul1819$ym <- "201907"
aug1819 <- calculateAutoMonths(ftautocomms,"2019-06-30")
aug1819$ym <- "201908"
sep1819 <- calculateAutoMonths(ftautocomms,"2019-07-31")
sep1819$ym <- "201909"
oct1819 <- calculateAutoMonths(ftautocomms,"2019-08-31")
oct1819$ym <- "201910"
nov1819 <- calculateAutoMonths(ftautocomms,"2019-09-30")
nov1819$ym <- "201911"
dec1819 <- calculateAutoMonths(ftautocomms,"2019-10-31")
dec1819$ym <- "201912"

ftauto_comms <- rbind(jul1819, aug1819, sep1819, oct1819, nov1819, dec1819)
write.csv(ftauto_comms, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FTAutomation.csv", row.names = FALSE)
