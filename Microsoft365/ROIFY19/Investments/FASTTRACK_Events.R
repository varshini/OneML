library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
options(stringsAsFactors = FALSE)

# Final set of shortlisted FT events - 105 unique events 
ft_shortlisted_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FinalEvents_Grouped.csv") # 105 events
ft_shortlisted_events <- ft_shortlisted_events %>% filter(!(ServiceNameEvent %in% c("SfB","Yammer", "Exchange: Outlook Mobile"))) # 95 events


tenant_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")
tenant_events_temp <- tenant_events
tenant_events_temp$MonthEndEventDate <- as.Date(mdy_hms(tenant_events_temp$MonthEndEventDate))
tenant_events_temp$EventTitle <- tolower(tenant_events_temp$EventTitle)
tenant_events_temp$EventTitle <- str_replace_all(string=tenant_events_temp$EventTitle, pattern=" ", repl="")

##1) TODO - FTC tasks - try it out
ft_events3 <- filter(ft_shortlisted_events, EventTitle %in% c("FTC Tasks-AADP", "FTC Tasks-OfficeProPlus", "FTC Tasks-Exchange"))
tenant_events_temp1 <- filter(tenant_events_temp, tenant_events_temp$EventCategory == "FTC Tasks" & tenant_events_temp$ServiceNameEvent %in% 
                                c("Exchange", "OPP", "AADP"))
tenant_events_temp1$EventTitle <- ifelse(tenant_events_temp$ServiceNameEvent == "Exchange", "FTC Tasks-Exchange", tenant_events_temp1$EventTitle)
tenant_events_temp1$EventTitle <- ifelse(tenant_events_temp$ServiceNameEvent == "AADP","FTC Tasks-AADP", tenant_events_temp1$EventTitle)
tenant_events_temp1$EventTitle <- ifelse(tenant_events_temp$ServiceNameEvent == "OPP","FTC Tasks-OfficeProPlus", tenant_events_temp1$EventTitle)
ft_events3 <- inner_join(tenant_events_temp1, ft_events3, by = c("EventTitle", "EventCategory", "ServiceNameEvent"))

##2) TODO - FT field notification (data to be backfilled)

# 3) TODO - SSAT fix needed - try it out
ft_events1 <- inner_join(tenant_events_temp, ft_shortlisted_events, by = c("EventTitle", "EventCategory", "ServiceNameEvent"))
ft_events2 <- filter(ft_shortlisted_events, EventTitle %in% c("SSAT-SfB(Skype for Business)", "SSAT-OneDrive", "SSAT-SharePoint","SSAT-Intune","SSAT-Yammer"))
ft_events2 <- inner_join(tenant_events_temp, ft_events2, by = c("EventCategory", "ServiceNameEvent"))

# FIX Bot
ft_shortlisted_events$EventTitle <- ifelse(ft_shortlisted_events$EventCategory == 'Admin Center-BOT', 'BOT', ft_shortlisted_events$EventTitle)
ft_shortlisted_events$EventCategory <- ifelse(ft_shortlisted_events$EventCategory == 'Admin Center-BOT', 'BOT', ft_shortlisted_events$EventCategory)
ft_shortlisted_events$EventTitle <- ifelse(ft_shortlisted_events$EventTitle == "BOT-Unknown", "BOT-Any Service", ft_shortlisted_events$EventTitle)



ft_shortlisted_events$EventTitle <- tolower(ft_shortlisted_events$EventTitle) 
ft_shortlisted_events$EventTitle <- str_replace_all(string=ft_shortlisted_events$EventTitle, pattern=" ", repl="")

ft_events <- rbind(ft_events1, ft_events2, ft_events3) # make small and strip space from 1, 2 and 3 

# a <- setdiff(ft_shortlisted_events$EventTitle, ft_events$EventTitle)
# length(a)
# View(a)


# 3) TODO - Add to tenant data Final event title - grouped event + category + service name event

ft_events_temp <- ft_events %>% select(TenantId, GroupedEvent, ServiceNameEvent, MonthEndEventDate) # 4) TODO -Need to change grouped event to grouped event final

# Calculate for every event - 1) # of times event occured in L-3, L-6, L-12 2) # of distinct months event occured in L-3, L-6, L-12
# Do this for all wkl, core wkl, modern comms

# TODO - 5) Calculate features
###############################################
# for filtering for specific workloads 
all_wkl <- c("ProPlus", "Teams","SPO", "EMS - AADP", "EMS - Intune", "AADP", "Microsoft Intune", "exo", "spo", "teams", "intune", "proplus", "aadp")
all_wkl <- data.frame(all_wkl)
colnames(all_wkl) <- "ServiceNameEvent"

core_wkl <- c("ProPlus", "SPO", "exo", "spo", "proplus")
core_wkl <- data.frame(core_wkl)
colnames(core_wkl) <- "ServiceNameEvent"

modern_comms <- c("Teams", "teams")
modern_comms <- data.frame(modern_comms)
colnames(modern_comms) <- "ServiceNameEvent"

###############################################

calculateDistinctMonths <- function(data, start_date1, start_date2, start_date3, end_date, var1, var2, var3)
{
  
  start_date <- "2018-08-01"
  end_date <- "2019-04-30"
  data <- all_wkl
  
  # t - 12
  data1 <- data %>% filter(MonthEndEventDate >= start_date1 & MonthEndEventDate <= end_date)
  data11 <- data1 %>% group_by(TenantId, GroupedEvent, MonthEndEventDate) %>% slice(1)
  data11 <- data11 %>% group_by(TenantId, GroupedEvent) %>% summarise(NoOfDistinctMonths= n())
  data12 <- spread(data11, GroupedEvent, NoOfDistinctMonths)
  colnames(data12) <- paste(colnames(data12), "t12", sep = "")
  
  # t - 6
  data1 <- data %>% filter(MonthEndEventDate >= start_date2 & MonthEndEventDate <= end_date)
  data11 <- data1 %>% group_by(TenantId, GroupedEvent, MonthEndEventDate) %>% slice(1)
  data11 <- data11 %>% group_by(TenantId, GroupedEvent) %>% summarise(NoOfDistinctMonths= n())
  data6 <- spread(data11, GroupedEvent, NoOfDistinctMonths)
  colnames(data6) <- paste(colnames(data6), "t6", sep = "")
  
  # t - 3
  data1 <- data %>% filter(MonthEndEventDate >= start_date3 & MonthEndEventDate <= end_date)
  data11 <- data1 %>% group_by(TenantId, GroupedEvent, MonthEndEventDate) %>% slice(1)
  data11 <- data11 %>% group_by(TenantId, GroupedEvent) %>% summarise(NoOfDistinctMonths= n())
  data3 <- spread(data11, GroupedEvent, NoOfDistinctMonths)
  colnames(data3) <- paste(colnames(data3), "t3", sep = "")
  
  data <- full_join(data12, data6, by = "TenantId") %>% 
    full_join(., data3, by = "TenantId")
  
  data[is.na(data)] <- 0
  
  return (data)
  
}

calculateEventCount <- function(data, start_date1, start_date2, start_date3, end_date, var1, var2, var3)
{
  
  start_date <- "2018-08-01"
  end_date <- "2019-04-30"
  data <- all_wkl
  
  # t - 12
  data1 <- data %>% filter(MonthEndEventDate >= start_date1 & MonthEndEventDate <= end_date)
  data1 <- data1 %>% group_by(TenantId, GroupedEvent) %>% summarise(NoOfTimes= n())
  data12 <- spread(data1, GroupedEvent, NoOfTimes)
  colnames(data12) <- paste(colnames(data12), "t12", sep = "")
  
  # t - 6
  data1 <- data %>% filter(MonthEndEventDate >= start_date2 & MonthEndEventDate <= end_date)
  data1 <- data1 %>% group_by(TenantId, GroupedEvent) %>% summarise(NoOfTimes= n())
  data6 <- spread(data1, GroupedEvent, NoOfTimes)
  colnames(data6) <- paste(colnames(data6), "t6", sep = "")
  
  # t - 3
  data1 <- data %>% filter(MonthEndEventDate >= start_date3 & MonthEndEventDate <= end_date)
  data1 <- data1 %>% group_by(TenantId, GroupedEvent) %>% summarise(NoOfTimes= n())
  data3 <- spread(data1, GroupedEvent, NoOfTimes)
  colnames(data3) <- paste(colnames(data3), "t3", sep = "")
  
  data <- full_join(data12, data6, by = "TenantId") %>% 
    full_join(., data3, by = "TenantId")
  
  data[is.na(data)] <- 0
  
  return (data)
  
}


# Repeat for all months and all combinations of workloads 
all_wkl <- inner_join(ft_events_temp, all_wkl, by = "ServiceNameEvent")
jul1819_1 <- calculateDistinctMonths(all_wkl, "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-30")
jul1819_2 <- calculateEventCount(all_wkl, "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-30")


