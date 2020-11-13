library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
options(stringsAsFactors = FALSE)

tenant_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")
tenant_events_temp <- tenant_events
tenant_events_temp$MonthEndEventDate <- as.Date(mdy_hms(tenant_events_temp$MonthEndEventDate))
tenant_events_temp <- tenant_events_temp %>% filter(AttributableToGain == "Yes" & 
                                                      EventCategory %in% c("Admin Center Wizards", "Admin Center-BOT", "Admin Center-Setup Guide",
                                                                           "Adoption Workshop","BOT","BOT Driven Wizards","E-Mail Your Org",
                                                                           "FastTrack Automation","Field - CSU Actions","FT Engagement","FT Migration Data",
                                                                           "FT Playbook Events","FT Playbook Milestones","FTC Tasks","FTOP Wizard", 
                                                                           "Service Task", "SSAT"))
tenant_events_temp <- tenant_events_temp %>% select(TenantId, ServiceNameEvent, MonthEndEventDate)

###############################################

# for filtering for specific workloads 
all_wkl <- c("Tenant Level","Exchange", "SPO", "OPP", "ODB", "Teams", "AADP", "Intune", "Exchange: Outlook Mobile","Office 365")
all_wkl <- data.frame(all_wkl)
colnames(all_wkl) <- "ServiceNameEvent"

core_wkl <- c("Tenant Level","Exchange", "SPO", "OPP", "ODB", "AADP", "Intune", "Exchange: Outlook Mobile","Office 365")
core_wkl <- data.frame(core_wkl)
colnames(core_wkl) <- "ServiceNameEvent"

modern_comms <- c("Tenant Level", "Teams","Office 365")
modern_comms <- data.frame(modern_comms)
colnames(modern_comms) <- "ServiceNameEvent"

aadp <- c("Tenant Level", "AADP", "AADP-P2", "EMS")

###############################################

# calculate # of months where some event took place 
calculateMonths <- function(data, start_date, end_date)
{
  # start_date <- "2018-08-01"
  # end_date <- "2019-05-01"
  # data <- all_wkl
  
  data <- data %>% filter(MonthEndEventDate >= start_date & MonthEndEventDate <= end_date)
  data1 <- data %>% group_by(TenantId, MonthEndEventDate) %>% slice(1)
  data1 <- data1 %>% group_by(TenantId) %>% summarise(FTEventMonths = n())
  return(data1)
}


###### Repeat for all months and all combinations of workloads ######
all_wkl <- inner_join(tenant_events_temp, all_wkl, by = "ServiceNameEvent")

jul1819_1 <- calculateMonths(all_wkl, "2018-08-01", "2019-05-31")
jul1819_1$ym <- "201907"

aug1819_1 <- calculateMonths(all_wkl, "2018-09-01", "2019-06-30")
aug1819_1$ym <- "201908"

sep1819_1 <- calculateMonths(all_wkl, "2018-10-01", "2019-07-31")
sep1819_1$ym <- "201909"

oct1819_1 <- calculateMonths(all_wkl, "2018-11-01", "2019-08-31")
oct1819_1$ym <- "201910"

nov1819_1 <- calculateMonths(all_wkl, "2018-12-01", "2019-09-30")
nov1819_1$ym <- "201911"

dec1819_1 <- calculateMonths(all_wkl, "2019-01-01", "2019-10-31")
dec1819_1$ym <- "201912"

all_wkl_1 <- rbind(jul1819_1, aug1819_1, sep1819_1, oct1819_1, nov1819_1, dec1819_1)
all_wkl_1$ym <- as.integer(all_wkl_1$ym)
all_wkl_1 <- left_join(mau_pau_tenants, all_wkl_1, by = c("TenantId","ym"))
all_wkl_1[is.na(all_wkl_1)] <- 0
write.csv(all_wkl_1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventAssignmentMonths.csv", row.names = FALSE) 


teams <- inner_join(tenant_events_temp, modern_comms, by = "ServiceNameEvent")

jul1819_1 <- calculateMonths(teams, "2018-08-01", "2019-05-31")
jul1819_1$ym <- "201907"

aug1819_1 <- calculateMonths(teams, "2018-09-01", "2019-06-30")
aug1819_1$ym <- "201908"

sep1819_1 <- calculateMonths(teams, "2018-10-01", "2019-07-31")
sep1819_1$ym <- "201909"

oct1819_1 <- calculateMonths(teams, "2018-11-01", "2019-08-31")
oct1819_1$ym <- "201910"

nov1819_1 <- calculateMonths(teams, "2018-12-01", "2019-09-30")
nov1819_1$ym <- "201911"

dec1819_1 <- calculateMonths(teams, "2019-01-01", "2019-10-31")
dec1819_1$ym <- "201912"

all_wkl_1 <- rbind(jul1819_1, aug1819_1, sep1819_1, oct1819_1, nov1819_1, dec1819_1)
all_wkl_1$ym <- as.integer(all_wkl_1$ym)
all_wkl_1 <- left_join(mau_pau_tenants, all_wkl_1, by = c("TenantId","ym"))
all_wkl_1[is.na(all_wkl_1)] <- 0
write.csv(all_wkl_1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEventAssignmentMonths.csv", row.names = FALSE) 

corewkl <- inner_join(tenant_events_temp, core_wkl, by = "ServiceNameEvent")

jul1819_1 <- calculateMonths(corewkl, "2018-08-01", "2019-05-31")
jul1819_1$ym <- "201907"

aug1819_1 <- calculateMonths(corewkl, "2018-09-01", "2019-06-30")
aug1819_1$ym <- "201908"

sep1819_1 <- calculateMonths(corewkl, "2018-10-01", "2019-07-31")
sep1819_1$ym <- "201909"

oct1819_1 <- calculateMonths(corewkl, "2018-11-01", "2019-08-31")
oct1819_1$ym <- "201910"

nov1819_1 <- calculateMonths(corewkl, "2018-12-01", "2019-09-30")
nov1819_1$ym <- "201911"

dec1819_1 <- calculateMonths(corewkl, "2019-01-01", "2019-10-31")
dec1819_1$ym <- "201912"

all_wkl_1 <- rbind(jul1819_1, aug1819_1, sep1819_1, oct1819_1, nov1819_1, dec1819_1)
all_wkl_1$ym <- as.integer(all_wkl_1$ym)
all_wkl_1 <- left_join(mau_pau_tenants, all_wkl_1, by = c("TenantId","ym"))
all_wkl_1[is.na(all_wkl_1)] <- 0
write.csv(all_wkl_1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEventAssignmentMonths.csv", row.names = FALSE) 
