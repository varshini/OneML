# Script to pull the features out of the models from Data bricks and analyse features and map the event names to readable format
library(dplyr)
tenant_level <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\TenantEvents.csv")

# Map the event names to regex replaced names 
#events <- tenant_level %>% select(EventTitle)
events <- distinct(tenant_level, EventTitle, EventCategory, ServiceNameEvent)

events$EventTitle1 <- tolower(events$EventTitle) 
events$EventTitle1 <- gsub('[^A-Za-z0-9]+', "",events$EventTitle1)
events$EventTitle1 <- paste(events$EventTitle1, events$EventCategory, events$ServiceNameEvent, sep = "")

classification20 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\classification_20.csv")
classification20 <- classification20 %>% filter(importance > 0)
colnames(classification20)[1] <- "EventTitle1"
shortlisted_events <- inner_join(classification20, events, by = "EventTitle1")
write.csv(shortlisted_events, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\ShortlistedEvents_new.csv", row.names = FALSE)

FinalInScopeEvents <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FinalEvents.csv")
colnames(FinalInScopeEvents)[1] <- "EventTitle"

# Due to some syntax issues, 2 are missing - otherwise all of them are there. Use original shortlisted_events_new file. 
final_data <- inner_join(FinalInScopeEvents, shortlisted_events, by = "EventTitle")

# Grouping events #
shortlisted_events$GroupedEvent <- case_when(grepl("Exchange Task", shortlisted_events$EventTitle) == TRUE ~ "Exchange Task", 
                                             grepl("Intune Task", shortlisted_events$EventTitle) == TRUE ~ "Intune Task", 
                                             grepl("OfficeProPlus Task", shortlisted_events$EventTitle) == TRUE ~ "OfficeProPlus Task", 
                                             grepl("Tenant Level Task", shortlisted_events$EventTitle) == TRUE ~ "Tenant Level Task", 
                                             grepl("Teams Task", shortlisted_events$EventTitle) == TRUE ~ "Teams Task", 
                                             grepl("AADP Task", shortlisted_events$EventTitle) == TRUE ~ "AADP Task", 
                                             grepl("SharePoint Task", shortlisted_events$EventTitle) == TRUE ~ "SharePoint Task",
                                             grepl("OneDrive Task", shortlisted_events$EventTitle) == TRUE ~ "OneDrive Task",
  
                                             grepl("BOT-AADP", shortlisted_events$EventTitle) == TRUE ~ "BOT-AADP",
                                             grepl("BOT-OneDrive", shortlisted_events$EventTitle) == TRUE ~ "BOT-ODB",
                                             grepl("BOT-Office 365", shortlisted_events$EventTitle) == TRUE ~ "BOT-Office365",
                                             grepl("BOT-OfficeProPlus", shortlisted_events$EventTitle) == TRUE ~ "BOT-OPP",
                                             grepl("BOT-SfB(Skype for Business)", shortlisted_events$EventTitle) == TRUE ~ "BOT-SfB",
                                             grepl("BOT-Teams", shortlisted_events$EventTitle) == TRUE ~ "BOT-Teams", 
                                             TRUE ~ shortlisted_events$EventTitle)
                                             
# shortlisted_events$GroupedEvent <-  ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "AADP", "BOT-AADP", 
#                                              ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "Tenant Level", "BOT-TenantLevel",
#                                                     ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "Exchange" ,"BOT-Exchange",
#                                                            ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "OPP" , "BOT-OPP",
#                                                                   ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "Teams" , "BOT-Teams",
#                                                                          ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "SPO" , "BOT-SPO",
#                                                                                 ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "SfB" , "BOT-SfB",
#                                                                                        ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "AADP" , "BOT-AADP",
#                                                                                               ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "ODB" , "BOT-ODB",
#                                                                                                      ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "Exchange: Outlook Mobile" , "BOT-OLM",
#                                                                                                             ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "Yammer" , "BOT-Yammer",
#                                                                                                                    ifelse(shortlisted_events$EventTitle == "BOT" & shortlisted_events$ServiceEventName == "Office 365" , "BOT-Office365",shortlisted_events$GroupedEvent)))))))))))) 
#                                              
#                                              
                                             
                                             

write.csv(shortlisted_events, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\FTEventsAnalysis\\FinalEvents_Grouped.csv")






