# Computed 2 kinds of features - No of months, Count of events - They were computed for 3 different time cohorts #

library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
options(stringsAsFactors = FALSE)

tenant_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")
tenant_events_temp <- tenant_events
tenant_events_temp$MonthEndEventDate <- as.Date(mdy_hms(tenant_events_temp$MonthEndEventDate))
tenant_events_temp$EventTitle <- tolower(tenant_events_temp$EventTitle)
tenant_events_temp$EventTitle <- str_replace_all(string=tenant_events_temp$EventTitle, pattern=" ", repl="")

# Final set of shortlisted FT events - 105 unique events 
ft_shortlisted_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FinalEvents_Grouped.csv") # 105 events
ft_shortlisted_events <- ft_shortlisted_events %>% filter(!(ServiceNameEvent %in% c("SfB","Yammer"))) # 98 grouped events

###### Fix mismatch in Event Titles ##########

# Fix Bot
ft_shortlisted_events$ServiceNameEvent <- ifelse(ft_shortlisted_events$EventTitle  == "BOT-Microsoft Defender ATP", "Microsoft Defender ATP", ft_shortlisted_events$ServiceNameEvent)
ft_shortlisted_events$ServiceNameEvent <- ifelse(ft_shortlisted_events$EventTitle  == "BOT-Windows", "Windows", ft_shortlisted_events$ServiceNameEvent)
ft_shortlisted_events$EventTitle <- ifelse(ft_shortlisted_events$EventCategory == 'Admin Center-BOT', 'BOT', ft_shortlisted_events$EventTitle)
ft_shortlisted_events$EventCategory <- ifelse(ft_shortlisted_events$EventCategory == 'Admin Center-BOT', 'BOT', ft_shortlisted_events$EventCategory)
ft_shortlisted_events$EventTitle <- ifelse(ft_shortlisted_events$EventTitle == "BOT-Unknown", "BOT-Any Service", ft_shortlisted_events$EventTitle)

#Fix SSAT
tenant_events_temp$EventTitle <- ifelse(tenant_events_temp$EventCategory == "SSAT", "ssat", tenant_events_temp$EventTitle)
ft_shortlisted_events$EventTitle <- ifelse(ft_shortlisted_events$EventCategory == "SSAT", "SSAT", ft_shortlisted_events$EventTitle)

# Fix FTC Tasks
ft_events1 <- filter(ft_shortlisted_events, EventTitle %in% c("FTC Tasks-AADP", "FTC Tasks-OfficeProPlus", "FTC Tasks-Exchange"))
tenant_events_temp1 <- filter(tenant_events_temp, tenant_events_temp$EventCategory == "FTC Tasks" & tenant_events_temp$ServiceNameEvent %in% 
                                c("Exchange", "OPP", "AADP"))
# Any exchange FTC tasks counts towards this, any AADP tasks counts towards this
tenant_events_temp1$EventTitle <- ifelse(tenant_events_temp1$ServiceNameEvent == "Exchange", "FTC Tasks-Exchange", tenant_events_temp1$EventTitle)
tenant_events_temp1$EventTitle <- ifelse(tenant_events_temp1$ServiceNameEvent == "AADP","FTC Tasks-AADP", tenant_events_temp1$EventTitle)
tenant_events_temp1$EventTitle <- ifelse(tenant_events_temp1$ServiceNameEvent == "OPP","FTC Tasks-OfficeProPlus", tenant_events_temp1$EventTitle)
ft_events1 <- inner_join(tenant_events_temp1, ft_events1, by = c("EventTitle", "EventCategory", "ServiceNameEvent"))
ft_events1$EventTitle <- tolower(ft_events1$EventTitle) 
ft_events1$EventTitle <- str_replace_all(string=ft_events1$EventTitle, pattern=" ", repl="")

# Field Notification - not including this as events were stopped being collected #

###### Fix mismatch in Event Titles ##########

ft_shortlisted_events$EventTitle <- tolower(ft_shortlisted_events$EventTitle) 
ft_shortlisted_events$EventTitle <- str_replace_all(string=ft_shortlisted_events$EventTitle, pattern=" ", repl="")

ft_events <- inner_join(tenant_events_temp, ft_shortlisted_events, by = c("EventTitle", "EventCategory", "ServiceNameEvent"))

ft_events_final <- rbind(ft_events, ft_events1)
#### I have 90 unique event titles at the end of this, 8 excluded because of FT field notification, 88 grouped event titles ####

# Fix Grouped Event based on all the changes and mismatches
ft_events_final$GroupedEvent <- ifelse(ft_events_final$GroupedEvent == "Awareness", "ssat", ft_events_final$GroupedEvent)
ft_events_final$GroupedEvent <- ifelse(ft_events_final$EventTitle == "prepareyourenvironment", "Prepare your environment", ft_events_final$GroupedEvent)

ft_events_final$FinalEventName <- paste(ft_events_final$GroupedEvent, ft_events_final$EventCategory, ft_events_final$ServiceNameEvent, sep = "")
ft_events_final$FinalEventName <- str_replace_all(ft_events_final$FinalEventName, pattern=" ", repl="")

final_ft_event_mapping <- unique(ft_events_final[c("EventTitle","EventCategory", "ServiceNameEvent", "GroupedEvent", "FinalEventName")]) # save this file
write.csv(final_ft_event_mapping, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FTEventMapping.csv", row.names = FALSE)

## 88 unique grouped events (this does not include workload name), 100 unique final event names with workload and service category in the featurename ##

########## Data processing and cleaning #########
ft_events_final1 <- ft_events_final %>% filter(MonthEndEventDate >= "2018-08-01" & MonthEndEventDate <= "2019-10-31")
ft_events_final1 <- select(ft_events_final1, TenantId, MonthEndEventDate, FinalEventName, ServiceNameEvent)
write.csv(ft_events_final1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FTEventDataFiltered.csv", row.names = FALSE) # Final set of FT events going to be used

########## Feature computation #########

# Calculate for every event - 1) # of times event occured in L-3, L-6, L-10 2) # of distinct months event occured in L-3, L-6, L-10
# Do this for all wkl, core wkl, modern comms

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

###############################################

# tried t-3, 3 to 6, 6 to 10. But data is very sparse. I think I can make it t - 5, 5 to 10
calculateDistinctMonths <- function(data, start_date1, start_date2, end_date)
{

  # start_date1 <- "2018-08-01"
  # start_date2 <- "2019-01-01"
  # end_date <- "2019-04-30"
  # data <- all_wkl
  
  # t - 5 to t - 10
  data1 <- data %>% filter(MonthEndEventDate >= start_date1 & MonthEndEventDate < start_date2)
  data11 <- data1 %>% group_by(TenantId, FinalEventName, MonthEndEventDate) %>% slice(1)
  data11 <- data11 %>% group_by(TenantId, FinalEventName) %>% summarise(NoOfDistinctMonths= n())
  data510 <- spread(data11, FinalEventName, NoOfDistinctMonths)
  for(eventnames in 1:length(ft_events_names$FinalEventName)){
    
    event_name <- ft_events_names$FinalEventName[eventnames]
    if (!(event_name %in% colnames(data510)))
    {
      data510[event_name] <- 0
    }
  }
  colnames(data510) <- paste(colnames(data510), "t510", sep = "")
  colnames(data510)[1] <- "TenantId"
  
  # t - 5
  data1 <- data %>% filter(MonthEndEventDate >= start_date2 & MonthEndEventDate < end_date)
  data11 <- data1 %>% group_by(TenantId, FinalEventName, MonthEndEventDate) %>% slice(1)
  data11 <- data11 %>% group_by(TenantId, FinalEventName) %>% summarise(NoOfDistinctMonths= n())
  data5 <- spread(data11, FinalEventName, NoOfDistinctMonths)
  for(eventnames in 1:length(ft_events_names$FinalEventName)){
    
    event_name <- ft_events_names$FinalEventName[eventnames]
    if (!(event_name %in% colnames(data5)))
    {
      data5[event_name] <- 0
    }
  }
  colnames(data5) <- paste(colnames(data5), "t5", sep = "")
  colnames(data5)[1] <- "TenantId"
  
  # t - 10
  data1 <- data %>% filter(MonthEndEventDate >= start_date1 & MonthEndEventDate < end_date)
  data11 <- data1 %>% group_by(TenantId, FinalEventName, MonthEndEventDate) %>% slice(1)
  data11 <- data11 %>% group_by(TenantId, FinalEventName) %>% summarise(NoOfDistinctMonths= n())
  data10 <- spread(data11, FinalEventName, NoOfDistinctMonths)
  for(eventnames in 1:length(ft_events_names$FinalEventName)){
    
    event_name <- ft_events_names$FinalEventName[eventnames]
    if (!(event_name %in% colnames(data10)))
    {
      data10[event_name] <- 0
    }
  }
  colnames(data10) <- paste(colnames(data10), "t10", sep = "")
  colnames(data10)[1] <- "TenantId"
  
  data <- full_join(data510, data5, by = "TenantId") %>% 
    full_join(., data10, by = "TenantId")
  
  data[is.na(data)] <- 0
  
  return (data)
  
}

calculateEventCount <- function(data, start_date1, start_date2, end_date)
{
  
  # start_date1 <- "2018-08-01"
  # start_date2 <- "2019-01-01"
  # end_date <- "2019-04-30"
  # data <- all_wkl
  
  # t - 5 to t - 10
  data1 <- data %>% filter(MonthEndEventDate >= start_date1 & MonthEndEventDate < start_date2)
  data1 <- data1 %>% group_by(TenantId, FinalEventName) %>% summarise(NoOfTimes= n())
  data510 <- spread(data1, FinalEventName, NoOfTimes)
  for(eventnames in 1:length(ft_events_names$FinalEventName)){
    
    event_name <- ft_events_names$FinalEventName[eventnames]
    if (!(event_name %in% colnames(data510)))
    {
      data510[event_name] <- 0
    }
  }
  colnames(data510) <- paste(colnames(data510), "t510", sep = "")
  colnames(data510)[1] <- "TenantId"
  
  # t - 5
  data1 <- data %>% filter(MonthEndEventDate >= start_date2 & MonthEndEventDate < end_date)
  data1 <- data1 %>% group_by(TenantId, FinalEventName) %>% summarise(NoOfTimes= n())
  data5 <- spread(data1, FinalEventName, NoOfTimes)
  for(eventnames in 1:length(ft_events_names$FinalEventName)){
    
    event_name <- ft_events_names$FinalEventName[eventnames]
    if (!(event_name %in% colnames(data5)))
    {
      data5[event_name] <- 0
    }
  }
  colnames(data5) <- paste(colnames(data5), "t5", sep = "")
  colnames(data5)[1] <- "TenantId"
  
  # t - 10
  data1 <- data %>% filter(MonthEndEventDate >= start_date1 & MonthEndEventDate < end_date)
  data1 <- data1 %>% group_by(TenantId, FinalEventName) %>% summarise(NoOfTimes= n())
  data10 <- spread(data1, FinalEventName, NoOfTimes)
  for(eventnames in 1:length(ft_events_names$FinalEventName)){
    
    event_name <- ft_events_names$FinalEventName[eventnames]
    if (!(event_name %in% colnames(data10)))
    {
      data10[event_name] <- 0
    }
  }
  colnames(data10) <- paste(colnames(data10), "t10", sep = "")
  colnames(data10)[1] <- "TenantId"
  
  data <- full_join(data510, data5, by = "TenantId") %>% 
    full_join(., data10, by = "TenantId")
  
  data[is.na(data)] <- 0
  
  return (data)
}

# Get all the unique event names to make sure all data frames have the same column names 
ft_events_names <- unique(ft_events_final1$FinalEventName)
ft_events_names <- data.frame(unique(ft_events_final1$FinalEventName))
colnames(ft_events_names)[1] <- "FinalEventName"

###### Repeat for all months and all combinations of workloads ######
all_wkl <- inner_join(ft_events_final1, all_wkl, by = "ServiceNameEvent")

jul1819_1 <- calculateDistinctMonths(all_wkl, "2018-08-01", "2019-01-01", "2019-05-31")
jul1819_1$ym <- "201907"

aug1819_1 <- calculateDistinctMonths(all_wkl, "2018-09-01", "2019-02-01", "2019-06-30")
aug1819_1$ym <- "201908"

sep1819_1 <- calculateDistinctMonths(all_wkl, "2018-10-01", "2019-03-01", "2019-07-31")
sep1819_1$ym <- "201909"

oct1819_1 <- calculateDistinctMonths(all_wkl, "2018-11-01", "2019-04-01", "2019-08-31")
oct1819_1$ym <- "201910"

nov1819_1 <- calculateDistinctMonths(all_wkl, "2018-12-01", "2019-05-01", "2019-09-30")
nov1819_1$ym <- "201911"

dec1819_1 <- calculateDistinctMonths(all_wkl, "2019-01-01", "2019-06-01", "2019-10-31")
dec1819_1$ym <- "201912"

all_wkl_1 <- rbind(jul1819_1, aug1819_1, sep1819_1, oct1819_1, nov1819_1, dec1819_1)
write.csv(all_wkl_1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventMonths.csv", row.names = FALSE)  

jul1819_2 <- calculateEventCount(all_wkl, "2018-08-01", "2019-01-01","2019-05-31")
jul1819_2$ym <- "201907"

aug1819_2 <- calculateEventCount(all_wkl, "2018-09-01", "2019-02-01", "2019-06-30")
aug1819_2$ym <- "201908"

sep1819_2 <- calculateEventCount(all_wkl, "2018-10-01", "2019-03-01", "2019-07-31")
sep1819_2$ym <- "201909"

oct1819_2 <- calculateEventCount(all_wkl, "2018-11-01", "2019-04-01", "2019-08-31")
oct1819_2$ym <- "201910"

nov1819_2 <- calculateEventCount(all_wkl, "2018-12-01", "2019-05-01", "2019-09-30")
nov1819_2$ym <- "201911"

dec1819_2 <- calculateEventCount(all_wkl, "2019-01-01", "2019-06-01", "2019-10-31")
dec1819_2$ym <- "201912"

all_wkl_2 <- rbind(jul1819_2, aug1819_2, sep1819_2, oct1819_2, nov1819_2, dec1819_2)
write.csv(all_wkl_2, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\AllWorkloads\\FasttrackEventCount.csv", row.names = FALSE)  

#--------
core_wkl <- inner_join(ft_events_final1, core_wkl, by = "ServiceNameEvent")

jul1819_1 <- calculateDistinctMonths(core_wkl, "2018-08-01", "2019-01-01", "2019-05-31")
jul1819_1$ym <- "201907"

aug1819_1 <- calculateDistinctMonths(core_wkl, "2018-09-01", "2019-02-01", "2019-06-30")
aug1819_1$ym <- "201908"

sep1819_1 <- calculateDistinctMonths(core_wkl, "2018-10-01", "2019-03-01", "2019-07-31")
sep1819_1$ym <- "201909"

oct1819_1 <- calculateDistinctMonths(core_wkl, "2018-11-01", "2019-04-01", "2019-08-31")
oct1819_1$ym <- "201910"

nov1819_1 <- calculateDistinctMonths(core_wkl, "2018-12-01", "2019-05-01", "2019-09-30")
nov1819_1$ym <- "201911"

dec1819_1 <- calculateDistinctMonths(core_wkl, "2019-01-01", "2019-06-01", "2019-10-31")
dec1819_1$ym <- "201912"

core_wkl_1 <- rbind(jul1819_1, aug1819_1, sep1819_1, oct1819_1, nov1819_1, dec1819_1)
write.csv(core_wkl_1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEventMonths.csv", row.names = FALSE)  

jul1819_2 <- calculateEventCount(core_wkl, "2018-08-01", "2019-01-01","2019-05-31")
jul1819_2$ym <- "201907"

aug1819_2 <- calculateEventCount(core_wkl, "2018-09-01", "2019-02-01", "2019-06-30")
aug1819_2$ym <- "201908"

sep1819_2 <- calculateEventCount(core_wkl, "2018-10-01", "2019-03-01", "2019-07-31")
sep1819_2$ym <- "201909"

oct1819_2 <- calculateEventCount(core_wkl, "2018-11-01", "2019-04-01", "2019-08-31")
oct1819_2$ym <- "201910"

nov1819_2 <- calculateEventCount(core_wkl, "2018-12-01", "2019-05-01", "2019-09-30")
nov1819_2$ym <- "201911"

dec1819_2 <- calculateEventCount(core_wkl, "2019-01-01", "2019-06-01", "2019-10-31")
dec1819_2$ym <- "201912"

core_wkl_2 <- rbind(jul1819_2, aug1819_2, sep1819_2, oct1819_2, nov1819_2, dec1819_2)
write.csv(core_wkl_2, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\CoreWorkloads\\FasttrackEventCount.csv", row.names = FALSE) 
#-----------

modern_comms <- inner_join(ft_events_final1, modern_comms, by = "ServiceNameEvent")

jul1819_1 <- calculateDistinctMonths(modern_comms, "2018-08-01", "2019-01-01", "2019-05-31")
jul1819_1$ym <- "201907"

aug1819_1 <- calculateDistinctMonths(modern_comms, "2018-09-01", "2019-02-01", "2019-06-30")
aug1819_1$ym <- "201908"

sep1819_1 <- calculateDistinctMonths(modern_comms, "2018-10-01", "2019-03-01", "2019-07-31")
sep1819_1$ym <- "201909"

oct1819_1 <- calculateDistinctMonths(modern_comms, "2018-11-01", "2019-04-01", "2019-08-31")
oct1819_1$ym <- "201910"

nov1819_1 <- calculateDistinctMonths(modern_comms, "2018-12-01", "2019-05-01", "2019-09-30")
nov1819_1$ym <- "201911"

dec1819_1 <- calculateDistinctMonths(modern_comms, "2019-01-01", "2019-06-01", "2019-10-31")
dec1819_1$ym <- "201912"

modern_comms_1 <- rbind(jul1819_1, aug1819_1, sep1819_1, oct1819_1, nov1819_1, dec1819_1)
write.csv(modern_comms_1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEventMonths.csv", row.names = FALSE)  

jul1819_2 <- calculateEventCount(modern_comms, "2018-08-01", "2019-01-01","2019-05-31")
jul1819_2$ym <- "201907"

aug1819_2 <- calculateEventCount(modern_comms, "2018-09-01", "2019-02-01", "2019-06-30")
aug1819_2$ym <- "201908"

sep1819_2 <- calculateEventCount(modern_comms, "2018-10-01", "2019-03-01", "2019-07-31")
sep1819_2$ym <- "201909"

oct1819_2 <- calculateEventCount(modern_comms, "2018-11-01", "2019-04-01", "2019-08-31")
oct1819_2$ym <- "201910"

nov1819_2 <- calculateEventCount(modern_comms, "2018-12-01", "2019-05-01", "2019-09-30")
nov1819_2$ym <- "201911"

dec1819_2 <- calculateEventCount(modern_comms, "2019-01-01", "2019-06-01", "2019-10-31")
dec1819_2$ym <- "201912"

modern_comms_2 <- rbind(jul1819_2, aug1819_2, sep1819_2, oct1819_2, nov1819_2, dec1819_2)
write.csv(modern_comms_2, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\FasttrackEventCount.csv", row.names = FALSE) 


