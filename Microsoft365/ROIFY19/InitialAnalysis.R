# Script to pull the features out of the models from Data bricks and analyse features and map the event names to readable format
library(dplyr)
tenant_level <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")

# Map the event names to regex replaced names 
#events <- tenant_level %>% select(EventTitle)
events <- distinct(tenant_level, EventTitle, EventCategory, ServiceNameEvent)

events$EventTitle1 <- tolower(events$EventTitle) 
events$EventTitle1 <- gsub('[^A-Za-z0-9]+', "",events$EventTitle1)
events$EventTitle1 <- paste(events$EventTitle1, events$EventCategory, events$ServiceNameEvent, sep = "")

classification10 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\classification_10_v2.csv")
classification10 <- classification10 %>% filter(importance > 0)
classification8filtered <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\classification_8_filtered_v2.csv")
classification8filtered <- classification8filtered %>% filter(importance > 0)
regression <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\Regression.csv")
regression <- regression %>% filter(importance > 0)

a <- inner_join(classification10, classification8filtered, by = "feature")
colnames(a)[1] <- "EventTitle1"
shortlisted_events <- inner_join(a, events, by = "EventTitle1")
write.csv(shortlisted_events, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\ShortlistedEvents_v2.csv", row.names = FALSE)

classification20 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\classification_20.csv")
classification20 <- classification20 %>% filter(importance > 0)
colnames(classification20)[1] <- "EventTitle1"
shortlisted_events <- inner_join(classification20, events, by = "EventTitle1")
write.csv(shortlisted_events, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\ShortlistedEvents_new.csv", row.names = FALSE)

shortlisted_events_old <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\ShortlistedEvents_old.csv")
colnames(shortlisted_events_old)[1] <- "EventTitle"

a <- inner_join(shortlisted_events_old, shortlisted_events)
b <- left_join(shortlisted_events, shortlisted_events_old)
# grouping tasks #
shortlisted_events$GroupedEvent <- case_when(grepl("Exchange Task", shortlisted_events$EventTitle) == TRUE ~ "Exchange Task", 
                                             grepl("Intune Task", shortlisted_events$EventTitle) == TRUE ~ "Intune Task", 
                                             grepl("OfficeProPlus Task", shortlisted_events$EventTitle) == TRUE ~ "OfficeProPlus Task", 
                                             grepl("Tenant Level Task", shortlisted_events$EventTitle) == TRUE ~ "Tenant Level Task", 
                                             grepl("Teams Task", shortlisted_events$EventTitle) == TRUE ~ "Teams Task", 
                                             grepl("AADP Task", shortlisted_events$EventTitle) == TRUE ~ "AADP Task", 
                                             grepl("SharePoint Task", shortlisted_events$EventTitle) == TRUE ~ "SharePoint Task",
                                             grepl("OneDrive Task", shortlisted_events$EventTitle) == TRUE ~ "OneDrive Task",
                                             grepl("Last Time", shortlisted_events$EventTitle) == TRUE ~ "Tenant Contact", 
                                             grepl("First Time Time", shortlisted_events$EventTitle) == TRUE ~ "Tenant Contact",
                                             TRUE ~ "NA")

write.csv(shortlisted_events, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\ShortlistedEvents_v2.csv", row.names = FALSE)

## CSM events counts ##
library(lubridate)
csm <- tenant_level %>% filter(EventTitle == "CSM Assigned")
csm$MonthEndDate <- mdy_hms(csm$MonthEndEventDate)
csm <- csm %>% filter(MonthEndDate > "2018-06-30" & MonthEndDate < "2019-07-01")



shortlisted_events <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\ShortlistedEvents_new.csv")
shortlisted_events <- shortlisted_events %>% filter(importance > 0)

a <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FinalEvents.csv")
colnames(a)[1] <- "EventTitle"
b <- inner_join(a, shortlisted_events, by = "EventTitle")

# library(dplyr)
# library(lubridate)
# library(CosmosToR)
# library(tidyr)
# 
# ################################
# # Picking the set of FT events #
# tenant_level <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\TenantEvents.csv")
tpid_level <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\TPIDEvents.csv")
# tenant_level1 <- tenant_level
# 
# tenant_level1 <- tenant_level1 %>% filter(MonthEndEventDate != "#NULL#")
# tenant_level1$MonthEndDate <- mdy_hms(tenant_level1$MonthEndEventDate)
# 
# tenant_level_filtered <- tenant_level1 %>% filter(MonthEndDate > "2018-06-30" & MonthEndDate < "2019-07-01")
# tenant_level_filtered <- tenant_level_filtered %>% filter(EventCategory %in%
#                                                           c('Admin Center Wizards',
#                                                           'Admin Center-BOT',
#                                                           'Admin Center-Setup Guide',
#                                                           'Adoption Workshop',
#                                                           'BOT',
#                                                           'BOT Driven Wizards',
#                                                           'E-Mail Your Org',
#                                                           'FastTrack Automation',
#                                                           'Field - CSU Actions',
#                                                           'FT Engagement',
#                                                           'FT Migration Data',
#                                                           'FT Playbook Events',
#                                                           'FT Playbook Milestones',
#                                                           'FTC Tasks',
#                                                           'FTOP Wizard',
#                                                           'Service Task',
#                                                           'SSAT'))
# 
# tenant_level_filtered <- tenant_level_filtered %>% filter(ServiceNameEvent %in%
#                                                             c('Tenant Level','ODB','SPO','Exchange: Outlook Mobile',
#                                                               'Intune','Exchange','Yammer','OPP','SfB',
#                                                               'SfB Cloud PSTN Conferencing','Teams','AADP','Office 365','SfB Cloud PBX',
#                                                               'SfB PSTN Calling'))
# 
# # Removing very rare occuring events - Mean - 2.5k, median - 43
# summary <- tenant_level_filtered %>% group_by(EventTitle) %>% summarise(count = n())
# nrow(filter(summary, count < 10)) #-292
# nrow(filter(summary, count < 20)) #-394
# nrow(filter(summary, count < 30)) #-447
# nrow(filter(summary, count < 40)) #-482
# nrow(filter(summary, count < 50)) #-514
# 
# filtered_events <- summary %>% filter(count > 50) %>% dplyr::select(EventTitle)
# 
# # filter out very small count events 
# 
# # Featurize the events - two types (1 or 0, # of events)
# tenant_level_filtered$EventLabel <- 1
# tenant_level_features <- tenant_level_filtered %>% group_by(TenantId, EventTitle) %>% summarise(#EventLabel = max(EventLabel))
#                                                                                                 EventCount = n())
# tenant_level_features <- inner_join(tenant_level_features, filtered_events, by = "EventTitle")
# 
# # long to wide 
# tenant_level_features1 <- tenant_level_features %>% spread(EventTitle, EventCount)
# tenant_level_features1[is.na(tenant_level_features1)] <- 0
# 
# tenant_level_features2 <- tenant_level_features %>% spread(EventTitle, EventLabel)
# tenant_level_features2[is.na(tenant_level_features2)] <- 0
# 
# ################################
# 
# # Pulling MAU for FY18 and FY19 for MAL
# 
# vc <- vc_connection('https://cosmos14.osdinfra.net/cosmos/ACE.proc/')
# streamPath <- "local/Projects/ROIFY19/FY18_19_MAU.ss"
# mau_data <- ss_all(vc, streamPath) # 100k tenants from 32k tpids with tenants and Office MAU as of FY18 and FY19
# mau_data$Growth <- (mau_data$MAUFY19 - mau_data$MAUFY18)/mau_data$MAUFY18
# mau_data$GrowthLabel <- ifelse(mau_data$Growth >= 0.01, "1", "0") # 56218 with label 1, 43931 with label 0
# colnames(mau_data)[1] <- "TenantId"
# mau_data <- mau_data %>% dplyr::select(TenantId, GrowthLabel)
# 
# ################################
# # Output variable - 0 or 1 based on >1% MAU Growth?
# # Input variables - 0 or 1 based on if the event happened or not 
# 
# final_data <- left_join(mau_data, tenant_level_features1, by = "TenantId")
# final_data[is.na(final_data)] <- 0
# 
# ################################
# # Logistic Regression Model 
# library(aod)
# final_data$GrowthLabel <- factor(final_data$GrowthLabel)
# final_data <- subset(final_data, select=-c(TenantId))
# 
# #final_data1 <- head(final_data, 1000)
# 
# mylogit <- glm(GrowthLabel~., data = final_data, family = "binomial")
# 
# #TODO - Try initial version with 1-0 label and event count
# # Try regression with actual mau value and event count 
# # See if yuo can use any tree based models 
# # What type of filtering on counts can you do?



