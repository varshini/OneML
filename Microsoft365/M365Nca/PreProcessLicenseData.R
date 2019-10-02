# Preprocess each type of license to one file and write out # 
# TODO - Windows #

library(dplyr)
library(readxl)
library(tidyr)
library(data.table)

preprocessDate <- function(data)
{
  data$ym1 <- case_when(data$ym %like% "FY17Q1" ~ "201609", 
                        data$ym %like% "FY17Q2" ~ "201612",
                        data$ym %like% "FY17Q3" ~ "201703",
                        data$ym %like% "FY17Q4" ~ "201706",
                        data$ym %like% "FY18Q1" ~ "201709",
                        data$ym %like% "FY18Q2" ~ "201712",
                        data$ym %like% "FY18Q3" ~ "201803",
                        data$ym %like% "FY18Q4" ~ "201806",
                        data$ym %like% "FY19Q1" ~ "201809",
                        data$ym %like% "FY19Q2" ~ "201812",
                        data$ym %like% "FY19Q3" ~ "201903",
                        data$ym %like% "FY19Q4" ~ "201906")
  return(data)
}

# m365 seats #

m365_seats1819 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\M365Seats.xlsx", sheet = "FY1819")
m365_seats17 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\M365Seats.xlsx", sheet = "FY17")
m365seats <- full_join(m365_seats17, m365_seats1819, by = "TPID")

m365seats$E3FY18Q2 <- m365seats$E3EnterpriseFY18Q4 + m365seats$BusinessSeatsFY18Q2 + m365seats$EDUSeatsFY18Q2
m365seats$E3FY18Q3 <- m365seats$E3EnterpriseFY18Q4 + m365seats$BusinessSeatsFY18Q3 + m365seats$EDUSeatsFY18Q3
m365seats$E3FY18Q4 <- m365seats$E3EnterpriseFY18Q4 + m365seats$BusinessSeatsFY18Q4 + m365seats$EDUSeatsFY18Q4
m365seats$E3FY19Q1 <- m365seats$E3EnterpriseFY19Q1 + m365seats$BusinessSeatsFY19Q1 + m365seats$EDUSeatsFY19Q1
m365seats$E3FY19Q2 <- m365seats$E3EnterpriseFY19Q2 + m365seats$BusinessSeatsFY19Q2 + m365seats$EDUSeatsFY19Q2
m365seats$E3FY19Q3 <- m365seats$E3EnterpriseFY19Q3 + m365seats$BusinessSeatsFY19Q3 + m365seats$EDUSeatsFY19Q3
m365seats$E3FY19Q4 <- m365seats$E3EnterpriseFY19Q4 + m365seats$BusinessSeatsFY19Q4 + m365seats$EDUSeatsFY19Q4

e3 <- m365seats %>% select(TPID, E3EnterpriseFY17Q1, E3EnterpriseFY17Q2, E3EnterpriseFY17Q3, E3EnterpriseFY17Q4, 
                          E3EnterpriseFY18Q1, E3FY18Q2, E3FY18Q3, E3FY18Q4,
                          E3FY19Q1, E3FY19Q2, E3FY19Q3, E3FY19Q4)

e5 <- m365seats %>% select(TPID, E5EnterpriseFY17Q2, E5EnterpriseFY17Q3, E5EnterpriseFY17Q4, 
                           E5EnterpriseFY18Q1, E5EnterpriseFY18Q2, E5EnterpriseFY18Q3, E5EnterpriseFY18Q4,
                           E5EnterpriseFY19Q1, E5EnterpriseFY19Q2, E5EnterpriseFY19Q3, E5EnterpriseFY19Q4)

f1 <- m365seats %>% select(TPID, F1SeatsFY18Q2, F1SeatsFY18Q3, F1SeatsFY18Q4,
                           F1SeatsFY19Q1, F1SeatsFY19Q2, F1SeatsFY19Q3, F1SeatsFY19Q4)

m365seats_e3 <- gather(e3, ym, M365E3, E3EnterpriseFY17Q1:E3FY19Q4)
m365seats_e5 <- gather(e5, ym, M365E5, E5EnterpriseFY17Q2:E5EnterpriseFY19Q4)
m365seats_f1 <- gather(f1, ym, M365F1, F1SeatsFY18Q2:F1SeatsFY19Q4)

m365seats_e3 <- preprocessDate(m365seats_e3) %>% select(TPID, M365E3, ym1)
m365seats_e5 <- preprocessDate(m365seats_e5) %>% select(TPID, M365E5, ym1)
m365seats_f1 <- preprocessDate(m365seats_f1) %>% select(TPID, M365F1, ym1)

m365 <- full_join(m365seats_e3, m365seats_e5, by = c("TPID", "ym1")) %>% 
  full_join(., m365seats_f1, by = c("TPID", "ym1"))

m365[is.na(m365)] <- 0
m365$TotalM365 <- m365$M365E3 + m365$M365E5 + m365$M365F1

write.csv(m365, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\M365Seats.csv", row.names = FALSE)

###########################################################################################################

# O365 seats # 

oq1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "FY17Q1")
oq2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "FY17Q2")
oq3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "FY17Q3")
oq4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "FY17Q4")
oq5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "18Q1")
oq6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "18Q2")
oq7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "18Q3")
oq8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "18Q4")
oq9 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "19Q1")
oq10 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "19Q2")
oq11 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "19Q3")
oq12 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\O365Seats.xlsx", sheet = "19Q4")

o365_seats <- full_join(oq1, oq2, by = "TPID") %>% 
              full_join(., oq3, by = "TPID") %>% 
              full_join(., oq4, by = "TPID") %>% 
              full_join(., oq5, by = "TPID") %>% 
              full_join(., oq6, by = "TPID") %>% 
              full_join(., oq7, by = "TPID") %>% 
              full_join(., oq8, by = "TPID") %>% 
              full_join(., oq9, by = "TPID") %>% 
              full_join(., oq10, by = "TPID") %>% 
              full_join(., oq11, by = "TPID") %>% 
              full_join(., oq12, by = "TPID")


e12 <- o365_seats %>% select(TPID, O365PlanE1E2FY17Q1, O365PlanE1E2FY17Q2, O365PlanE1E2FY17Q3, O365PlanE1E2FY17Q4, 
                           O365PlanE1E2FY18Q1, O365PlanE1E2FY18Q2, O365PlanE1E2FY18Q3, O365PlanE1E2FY18Q4,
                           O365PlanE1E2FY19Q1, O365PlanE1E2FY19Q2, O365PlanE1E2FY19Q3, O365PlanE1E2FY19Q4)

e12addon <- o365_seats %>% select(TPID, O365PlanE1E2AddOnFY17Q1, O365PlanE1E2AddOnFY17Q2, O365PlanE1E2AddOnFY17Q3, O365PlanE1E2AddOnFY17Q4, 
                                  O365PlanE1E2AddOnFY18Q1, O365PlanE1E2AddOnFY18Q2, O365PlanE1E2AddOnFY18Q3, O365PlanE1E2AddOnFY18Q4,
                                  O365PlanE1E2AddOnFY19Q1, O365PlanE1E2AddOnFY19Q2, O365PlanE1E2AddOnFY19Q3, O365PlanE1E2AddOnFY19Q4)

e3 <- o365_seats %>% select(TPID, O365PlanE3FY17Q1, O365PlanE3FY17Q2, O365PlanE3FY17Q3, O365PlanE3FY17Q4, 
                            O365PlanE3FY18Q1, O365PlanE3FY18Q2, O365PlanE3FY18Q3, O365PlanE3FY18Q4,
                            O365PlanE3FY19Q1, O365PlanE3FY19Q2, O365PlanE3FY19Q3, O365PlanE3FY19Q4)

e3addon <- o365_seats %>% select(TPID, O365PlanE3AddOnFY17Q1, O365PlanE3AddOnFY17Q2, O365PlanE3AddOnFY17Q3, O365PlanE3AddOnFY17Q4, 
                                 O365PlanE3AddOnFY18Q1, O365PlanE3AddOnFY18Q2, O365PlanE3AddOnFY18Q3, O365PlanE3AddOnFY18Q4,
                                 O365PlanE3AddOnFY19Q1, O365PlanE3AddOnFY19Q2, O365PlanE3AddOnFY19Q3, O365PlanE3AddOnFY19Q4)

e4 <- o365_seats %>% select(TPID, O365PlanE4FY17Q1, O365PlanE4FY17Q2, O365PlanE4FY17Q3, O365PlanE4FY17Q4, 
                            O365PlanE4FY18Q1, O365PlanE4FY18Q2, O365PlanE4FY18Q3, O365PlanE4FY18Q4,
                            O365PlanE4FY19Q1, O365PlanE4FY19Q2, O365PlanE4FY19Q3, O365PlanE4FY19Q4)

e4addon <- o365_seats %>% select(TPID, O365PlanE4AddOnFY17Q1, O365PlanE4AddOnFY17Q2, O365PlanE4AddOnFY17Q3, O365PlanE4AddOnFY17Q4, 
                                 O365PlanE4AddOnFY18Q1, O365PlanE4AddOnFY18Q2, O365PlanE4AddOnFY18Q3, O365PlanE4AddOnFY18Q4,
                                 O365PlanE4AddOnFY19Q1, O365PlanE4AddOnFY19Q2, O365PlanE4AddOnFY19Q3, O365PlanE4AddOnFY19Q4)

e5 <- o365_seats %>% select(TPID, O365PlanE5FY17Q1, O365PlanE5FY17Q2, O365PlanE5FY17Q3, O365PlanE5FY17Q4, 
                            O365PlanE5FY18Q1, O365PlanE5FY18Q2, O365PlanE5FY18Q3, O365PlanE5FY18Q4,
                            O365PlanE5FY19Q1, O365PlanE5FY19Q2, O365PlanE5FY19Q3, O365PlanE5FY19Q4)

e5addon <- o365_seats %>% select(TPID, O365PlanE5AddOnFY17Q1, O365PlanE5AddOnFY17Q2, O365PlanE5AddOnFY17Q3, O365PlanE5AddOnFY17Q4, 
                                 O365PlanE5AddOnFY18Q1, O365PlanE5AddOnFY18Q2, O365PlanE5AddOnFY18Q3, O365PlanE5AddOnFY18Q4,
                                 O365PlanE5AddOnFY19Q1, O365PlanE5AddOnFY19Q2, O365PlanE5AddOnFY19Q3, O365PlanE5AddOnFY19Q4)


o365seats_e12 <- gather(e12, ym, O365E12, O365PlanE1E2FY17Q1:O365PlanE1E2FY19Q4)
o365seats_e12add <- gather(e12addon, ym, O365E12add, O365PlanE1E2AddOnFY17Q1:O365PlanE1E2AddOnFY19Q4)
o365seats_e3 <- gather(e3, ym, O365E3, O365PlanE3FY17Q1:O365PlanE3FY19Q4)
o365seats_e3add <- gather(e3addon, ym, O365E3add, O365PlanE3AddOnFY17Q1:O365PlanE3AddOnFY19Q4)
o365seats_e4 <- gather(e4, ym, O365E4, O365PlanE4FY17Q1:O365PlanE4FY19Q4)
o365seats_e4add <- gather(e4addon, ym, O365E4add, O365PlanE4AddOnFY17Q1:O365PlanE4AddOnFY19Q4)
o365seats_e5 <- gather(e5, ym, O365E5, O365PlanE5FY17Q1:O365PlanE5FY19Q4)
o365seats_e5add <- gather(e5addon, ym, O365E5add, O365PlanE5AddOnFY17Q1:O365PlanE5AddOnFY19Q4)


o365seats_e12 <- preprocessDate(o365seats_e12) %>% select(TPID, O365E12, ym1)
o365seats_e12add <- preprocessDate(o365seats_e12add) %>% select(TPID, O365E12add, ym1)
o365seats_e3 <- preprocessDate(o365seats_e3) %>% select(TPID, O365E3, ym1)
o365seats_e3add <- preprocessDate(o365seats_e3add) %>% select(TPID, O365E3add, ym1)
o365seats_e4 <- preprocessDate(o365seats_e4) %>% select(TPID, O365E4, ym1)
o365seats_e4add <- preprocessDate(o365seats_e4add) %>% select(TPID, O365E4add, ym1)
o365seats_e5 <- preprocessDate(o365seats_e5) %>% select(TPID, O365E5, ym1)
o365seats_e5add <- preprocessDate(o365seats_e5add) %>% select(TPID, O365E5add, ym1)


o365 <- full_join(o365seats_e12, o365seats_e12add, by = c("TPID", "ym1")) %>% 
  full_join(., o365seats_e3, by = c("TPID", "ym1")) %>% 
  full_join(., o365seats_e3add, by = c("TPID", "ym1")) %>% 
  full_join(., o365seats_e4, by = c("TPID", "ym1")) %>% 
  full_join(., o365seats_e4add, by = c("TPID", "ym1")) %>% 
  full_join(., o365seats_e5, by = c("TPID", "ym1")) %>% 
  full_join(., o365seats_e5add, by = c("TPID", "ym1")) 

o365[is.na(o365)] <- 0
write.csv(o365, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\O365Seats.csv", row.names = FALSE)

###########################################################################################################

# EMS Seats #

oq1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "17Q1")
oq2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "17Q2")
oq3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "17Q3")
oq4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "17Q4")
oq5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "18Q1")
oq6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "18Q2")
oq7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "18Q3")
oq8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "18Q4")
oq9 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "19Q1")
oq10 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "19Q2")
oq11 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "19Q3")
oq12 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\EMSSeats.xlsx", sheet = "19Q4")

ems_seats <- full_join(oq1, oq2, by = "TPID") %>% 
  full_join(., oq3, by = "TPID") %>% 
  full_join(., oq4, by = "TPID") %>% 
  full_join(., oq5, by = "TPID") %>% 
  full_join(., oq6, by = "TPID") %>% 
  full_join(., oq7, by = "TPID") %>% 
  full_join(., oq8, by = "TPID") %>% 
  full_join(., oq9, by = "TPID") %>% 
  full_join(., oq10, by = "TPID") %>% 
  full_join(., oq11, by = "TPID") %>% 
  full_join(., oq12, by = "TPID")


aad <- ems_seats %>% select(TPID, AADFY17Q1, AADFY17Q2, AADFY17Q3, AADFY17Q4, 
                             AADFY18Q1, AADFY18Q2, AADFY18Q3, AADFY18Q4,
                             AADFY19Q1, AADFY19Q2, AADFY19Q3, AADFY19Q4)

aadp <- ems_seats %>% select(TPID, AADP2FY17Q1, AADP2FY17Q2, AADP2FY17Q3, AADP2FY17Q4, 
                            AADP2FY18Q1, AADP2FY18Q2, AADP2FY18Q3, AADP2FY18Q4,
                            AADP2FY19Q1, AADP2FY19Q2, AADP2FY19Q3, AADP2FY19Q4)

protprem <- ems_seats %>% select(TPID, AzureInfoProtPremP2FY17Q1, AzureInfoProtPremP2FY17Q2, AzureInfoProtPremP2FY17Q3, AzureInfoProtPremP2FY17Q4, 
                            AzureInfoProtPremP2FY18Q1, AzureInfoProtPremP2FY18Q2, AzureInfoProtPremP2FY18Q3, AzureInfoProtPremP2FY18Q4,
                            AzureInfoProtPremP2FY19Q1, AzureInfoProtPremP2FY19Q2, AzureInfoProtPremP2FY19Q3, AzureInfoProtPremP2FY19Q4)

mobidentity <- ems_seats %>% select(TPID, AzureMobIdentitySvcsFY17Q1, AzureMobIdentitySvcsFY17Q2, AzureMobIdentitySvcsFY17Q3, AzureMobIdentitySvcsFY17Q4, 
                            AzureMobIdentitySvcsFY18Q1, AzureMobIdentitySvcsFY18Q2, AzureMobIdentitySvcsFY18Q3, AzureMobIdentitySvcsFY18Q4,
                            AzureMobIdentitySvcsFY19Q1, AzureMobIdentitySvcsFY19Q2, AzureMobIdentitySvcsFY19Q3, AzureMobIdentitySvcsFY19Q4)

remoteapp <- ems_seats %>% select(TPID, AzureRemoteAppFY17Q1, AzureRemoteAppFY17Q2, AzureRemoteAppFY17Q3, AzureRemoteAppFY17Q4, 
                                    AzureRemoteAppFY18Q1, AzureRemoteAppFY18Q2, AzureRemoteAppFY18Q3, AzureRemoteAppFY18Q4,
                                    AzureRemoteAppFY19Q1, AzureRemoteAppFY19Q2, AzureRemoteAppFY19Q3, AzureRemoteAppFY19Q4)

rightsmgmt <- ems_seats %>% select(TPID, AzureRightsManagementServicesFY17Q1, AzureRightsManagementServicesFY17Q2, AzureRightsManagementServicesFY17Q3, AzureRightsManagementServicesFY17Q4, 
                                    AzureRightsManagementServicesFY18Q1, AzureRightsManagementServicesFY18Q2, AzureRightsManagementServicesFY18Q3, AzureRightsManagementServicesFY18Q4,
                                    AzureRightsManagementServicesFY19Q1, AzureRightsManagementServicesFY19Q2, AzureRightsManagementServicesFY19Q3, AzureRightsManagementServicesFY19Q4)

appsecurity <- ems_seats %>% select(TPID, CloudAppSecurityFY17Q1, CloudAppSecurityFY17Q2, CloudAppSecurityFY17Q3, CloudAppSecurityFY17Q4, 
                                    CloudAppSecurityFY18Q1, CloudAppSecurityFY18Q2, CloudAppSecurityFY18Q3, CloudAppSecurityFY18Q4,
                                    CloudAppSecurityFY19Q1, CloudAppSecurityFY19Q2, CloudAppSecurityFY19Q3, CloudAppSecurityFY19Q4)

emsksuite <- ems_seats %>% select(TPID,EMSCoreKSuiteFY18Q4,
                                  EMSCoreKSuiteFY19Q1, EMSCoreKSuiteFY19Q2, EMSCoreKSuiteFY19Q3, EMSCoreKSuiteFY19Q4)
  
emse3cao <- ems_seats %>% select(TPID, EMSE3SuiteCAOFY17Q1, EMSE3SuiteCAOFY17Q2, EMSE3SuiteCAOFY17Q3, EMSE3SuiteCAOFY17Q4, 
                                    EMSE3SuiteCAOFY18Q1, EMSE3SuiteCAOFY18Q2, EMSE3SuiteCAOFY18Q3, EMSE3SuiteCAOFY18Q4,
                                    EMSE3SuiteCAOFY19Q1, EMSE3SuiteCAOFY19Q2, EMSE3SuiteCAOFY19Q3, EMSE3SuiteCAOFY19Q4)

emse3fusl <- ems_seats %>% select(TPID, EMSE3SuiteFUSLFY17Q4, 
                              EMSE3SuiteFUSLFY18Q1, EMSE3SuiteFUSLFY18Q2, EMSE3SuiteFUSLFY18Q3, EMSE3SuiteFUSLFY18Q4,
                              EMSE3SuiteFUSLFY19Q1, EMSE3SuiteFUSLFY19Q2, EMSE3SuiteFUSLFY19Q3, EMSE3SuiteFUSLFY19Q4)

emse5fusl <- ems_seats %>% select(TPID, EMSE5SuiteFUSLFY17Q1, EMSE5SuiteFUSLFY17Q2, EMSE5SuiteFUSLFY17Q3, EMSE5SuiteFUSLFY17Q4, 
                              EMSE5SuiteFUSLFY18Q1, EMSE5SuiteFUSLFY18Q2, EMSE5SuiteFUSLFY18Q3, EMSE5SuiteFUSLFY18Q4,
                              EMSE5SuiteFUSLFY19Q1, EMSE5SuiteFUSLFY19Q2, EMSE5SuiteFUSLFY19Q3, EMSE5SuiteFUSLFY19Q4)

emse5cao <- ems_seats %>% select(TPID, 
                              EMSE5SuiteCAOFY18Q1, EMSE5SuiteCAOFY18Q2, EMSE5SuiteCAOFY18Q3, EMSE5SuiteCAOFY18Q4,
                              EMSE5SuiteCAOFY19Q1, EMSE5SuiteCAOFY19Q2, EMSE5SuiteCAOFY19Q3, EMSE5SuiteCAOFY19Q4)

emse5k <- ems_seats %>% select(TPID, 
                              EMSE5KSuiteFY18Q1, EMSE5KSuiteFY18Q2, EMSE5KSuiteFY18Q3, EMSE5KSuiteFY18Q4,
                              EMSE5KSuiteFY19Q1, EMSE5KSuiteFY19Q2, EMSE5KSuiteFY19Q3, EMSE5KSuiteFY19Q4)

intune <- ems_seats %>% select(TPID, IntuneFY17Q1, IntuneFY17Q2, IntuneFY17Q3, IntuneFY17Q4, 
                              IntuneFY18Q1, IntuneFY18Q2, IntuneFY18Q3, IntuneFY18Q4,
                              IntuneFY19Q1, IntuneFY19Q2, IntuneFY19Q3, IntuneFY19Q4)

intuneClient <- ems_seats %>% select(TPID, IntuneClientFY17Q1, IntuneClientFY17Q2, IntuneClientFY17Q3, IntuneClientFY17Q4, 
                              IntuneClientFY18Q1, IntuneClientFY18Q2, IntuneClientFY18Q3, IntuneClientFY18Q4,
                              IntuneClientFY19Q1, IntuneClientFY19Q2, IntuneClientFY19Q3, IntuneClientFY19Q4)

appsecurity <- gather(appsecurity, ym, CloudAppSecurity, CloudAppSecurityFY17Q1:CloudAppSecurityFY19Q4)
rightsmgmt <- gather(rightsmgmt, ym, RightsMgmt, AzureRightsManagementServicesFY17Q1:AzureRightsManagementServicesFY19Q4)
remoteapp <- gather(remoteapp, ym, RemoteApp, AzureRemoteAppFY17Q1:AzureRemoteAppFY19Q4)
mobidentity <- gather(mobidentity, ym, MobIdentity, AzureMobIdentitySvcsFY17Q1:AzureMobIdentitySvcsFY19Q4)
protprem <- gather(protprem, ym, InfoProtPrem, AzureInfoProtPremP2FY17Q1:AzureInfoProtPremP2FY19Q4)
aadp <- gather(aadp, ym, AADP2, AADP2FY17Q1:AADP2FY19Q4)
aad <- gather(aad, ym, AAD, AADFY17Q1:AADFY19Q4)
emsksuite <- gather(emsksuite, ym, EMSCoreKSuite, EMSCoreKSuiteFY18Q4:EMSCoreKSuiteFY19Q4)
emse3cao <- gather(emse3cao, ym, EMSE3Cao, EMSE3SuiteCAOFY17Q1:EMSE3SuiteCAOFY19Q4)
emse3fusl <- gather(emse3fusl, ym, EMSE3Fusl, EMSE3SuiteFUSLFY17Q4:EMSE3SuiteFUSLFY19Q4)
emse5fusl <- gather(emse5fusl, ym, EMSE5Fusl, EMSE5SuiteFUSLFY17Q1:EMSE5SuiteFUSLFY19Q4)
emse5cao <- gather(emse5cao, ym, EMSE5Cao, EMSE5SuiteCAOFY18Q1:EMSE5SuiteCAOFY19Q4)
emse5k <- gather(emse5k, ym, EMSE5KSuite, EMSE5KSuiteFY18Q1:EMSE5KSuiteFY19Q4)
intune <- gather(intune, ym, Intune, IntuneFY17Q1:IntuneFY19Q4)
intuneClient <- gather(intuneClient, ym, IntuneClient, IntuneClientFY17Q1:IntuneClientFY19Q4)

appsecurity <- preprocessDate(appsecurity) %>% select(TPID, CloudAppSecurity, ym1)
rightsmgmt <- preprocessDate(rightsmgmt)%>% select(TPID, RightsMgmt, ym1)
remoteapp <- preprocessDate(remoteapp)%>% select(TPID, RemoteApp, ym1)
mobidentity <- preprocessDate(mobidentity)%>% select(TPID, MobIdentity, ym1)
protprem <- preprocessDate(protprem)%>% select(TPID, InfoProtPrem, ym1)
aadp <- preprocessDate(aadp)%>% select(TPID, AADP2, ym1)
aad <- preprocessDate(aad)%>% select(TPID, AAD, ym1)
emsksuite <- preprocessDate(emsksuite)%>% select(TPID, EMSCoreKSuite, ym1)
emse3cao <- preprocessDate(emse3cao)%>% select(TPID, EMSE3Cao, ym1)
emse3fusl <- preprocessDate(emse3fusl)%>% select(TPID, EMSE3Fusl, ym1)
emse5fusl <- preprocessDate(emse5fusl)%>% select(TPID, EMSE5Fusl, ym1)
emse5cao <- preprocessDate(emse5cao)%>% select(TPID, EMSE5Cao, ym1)
emse5k <- preprocessDate(emse5k)%>% select(TPID, EMSE5KSuite, ym1)
intune <- preprocessDate(intune)%>% select(TPID, Intune, ym1)
intuneClient <- preprocessDate(intuneClient)%>% select(TPID, IntuneClient, ym1)

ems <- full_join(appsecurity, rightsmgmt, by = c("TPID", "ym1")) %>% 
  full_join(., remoteapp, by = c("TPID", "ym1")) %>% 
  full_join(., mobidentity, by = c("TPID", "ym1")) %>% 
  full_join(., protprem, by = c("TPID", "ym1"))%>% 
  full_join(., aadp, by = c("TPID", "ym1"))%>% 
  full_join(., aad, by = c("TPID", "ym1"))%>% 
  full_join(., emsksuite, by = c("TPID", "ym1"))%>% 
  full_join(., emse3cao, by = c("TPID", "ym1"))%>% 
  full_join(., emse3fusl, by = c("TPID", "ym1"))%>% 
  full_join(., emse5fusl, by = c("TPID", "ym1"))%>% 
  full_join(., emse5cao, by = c("TPID", "ym1"))%>% 
  full_join(., emse5k, by = c("TPID", "ym1"))%>% 
  full_join(., intune, by = c("TPID", "ym1"))%>% 
  full_join(., intuneClient, by = c("TPID", "ym1"))

ems[is.na(ems)] <- 0
write.csv(ems, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\EMSSeats.csv", row.names = FALSE)

###########################################################################################################

# OnPremSeats #

oq1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY17Q1")
oq2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY17Q2")
oq3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY17Q3")
oq4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY17Q4")
oq5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY18Q1")
oq6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY18Q2")
oq7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY18Q3")
oq8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY18Q4")
oq9 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY19Q1")
oq10 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY19Q2")
oq11 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY19Q3")
oq12 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\OnPremSeats.xlsx", sheet = "FY19Q4")

onprem_seats <- full_join(oq1, oq2, by = "TPID") %>% 
  full_join(., oq3, by = "TPID") %>% 
  full_join(., oq4, by = "TPID") %>% 
  full_join(., oq5, by = "TPID") %>% 
  full_join(., oq6, by = "TPID") %>% 
  full_join(., oq7, by = "TPID") %>% 
  full_join(., oq8, by = "TPID") %>% 
  full_join(., oq9, by = "TPID") %>% 
  full_join(., oq10, by = "TPID") %>% 
  full_join(., oq11, by = "TPID") %>% 
  full_join(., oq12, by = "TPID")

annuity <- onprem_seats %>% select(TPID, AnnuityFY17Q1, AnnuityFY17Q2, AnnuityFY17Q3, AnnuityFY17Q4, 
                                     AnnuityFY18Q1, AnnuityFY18Q2, AnnuityFY18Q3, AnnuityFY18Q4,
                                     AnnuityFY19Q1, AnnuityFY19Q2, AnnuityFY19Q3, AnnuityFY19Q4)

darkAnnuity <- onprem_seats %>% select(TPID, DarkAnnuityFY17Q1, DarkAnnuityFY17Q2, DarkAnnuityFY17Q3, DarkAnnuityFY17Q4, 
                                     DarkAnnuityFY18Q1, DarkAnnuityFY18Q2, DarkAnnuityFY18Q3, DarkAnnuityFY18Q4,
                                     DarkAnnuityFY19Q1, DarkAnnuityFY19Q2, DarkAnnuityFY19Q3, DarkAnnuityFY19Q4)

nonAnnuity <- onprem_seats %>% select(TPID, NonAnnuityFY17Q1, NonAnnuityFY17Q2, NonAnnuityFY17Q3, NonAnnuityFY17Q4, 
                                     NonAnnuityFY18Q1, NonAnnuityFY18Q2, NonAnnuityFY18Q3, NonAnnuityFY18Q4,
                                     NonAnnuityFY19Q1, NonAnnuityFY19Q2, NonAnnuityFY19Q3, NonAnnuityFY19Q4)

annuity <- gather(annuity, ym, Annuity, AnnuityFY17Q1:AnnuityFY19Q4)
darkAnnuity <- gather(darkAnnuity, ym, DarkAnnuity, DarkAnnuityFY17Q1:DarkAnnuityFY19Q4)
nonAnnuity <- gather(nonAnnuity, ym, NonAnnuity, NonAnnuityFY17Q1:NonAnnuityFY19Q4)

annuity <- preprocessDate(annuity) %>% select(TPID, ym1, Annuity)
nonAnnuity <- preprocessDate(nonAnnuity) %>% select(TPID, ym1, NonAnnuity)
darkAnnuity <- preprocessDate(darkAnnuity) %>% select(TPID, ym1, DarkAnnuity)

onprem <- full_join(annuity, nonAnnuity, by = c("TPID", "ym1")) %>% 
  full_join(., darkAnnuity, by = c("TPID", "ym1"))
onprem[is.na(onprem)] <- 0

write.csv(onprem, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\OnPremSeats.csv", row.names = FALSE)

###########################################################################################################

# Windows #

oq1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\WindowsSeats.xlsx", sheet = "FY17")
oq2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\WindowsSeats.xlsx", sheet = "FY18")
oq3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\WindowsSeats.xlsx", sheet = "FY19")

windows_seats <- full_join(oq1, oq2, by = "TPID") %>% 
  full_join(., oq3, by = "TPID")

win10perdevice <- windows_seats %>% select(TPID, Windows10PerDeviceFY17Q1, Windows10PerDeviceFY17Q2, Windows10PerDeviceFY17Q3, Windows10PerDeviceFY17Q4, 
                                   Windows10PerDeviceFY18Q1, Windows10PerDeviceFY18Q2, Windows10PerDeviceFY18Q3, Windows10PerDeviceFY18Q4,
                                   Windows10PerDeviceFY19Q1, Windows10PerDeviceFY19Q2, Windows10PerDeviceFY19Q3, Windows10PerDeviceFY19Q4)

win10peruser <- windows_seats %>% select(TPID, Windows10PerUserFY17Q1, Windows10PerUserFY17Q2, Windows10PerUserFY17Q3, Windows10PerUserFY17Q4, 
                                       Windows10PerUserFY18Q1, Windows10PerUserFY18Q2, Windows10PerUserFY18Q3, Windows10PerUserFY18Q4,
                                       Windows10PerUserFY19Q1, Windows10PerUserFY19Q2, Windows10PerUserFY19Q3, Windows10PerUserFY19Q4)

windowsclient <- windows_seats %>% select(TPID, WindowsClientFY17Q1, WindowsClientFY17Q2, WindowsClientFY17Q3, WindowsClientFY17Q4, 
                                      WindowsClientFY18Q1, WindowsClientFY18Q2, WindowsClientFY18Q3, WindowsClientFY18Q4,
                                      WindowsClientFY19Q1, WindowsClientFY19Q2, WindowsClientFY19Q3, WindowsClientFY19Q4)

windowse5 <- windows_seats %>% select(TPID, WindowsE5FY17Q1, WindowsE5FY17Q2, WindowsE5FY17Q3, WindowsE5FY17Q4, 
                                       WindowsE5FY18Q1, WindowsE5FY18Q2, WindowsE5FY18Q3, WindowsE5FY18Q4,
                                       WindowsE5FY19Q1, WindowsE5FY19Q2, WindowsE5FY19Q3, WindowsE5FY19Q4)

win10perdevice <- gather(win10perdevice, ym, Win10PerDevice, Windows10PerDeviceFY17Q1:Windows10PerDeviceFY19Q4)
win10peruser <- gather(win10peruser, ym, Win10PerUser, Windows10PerUserFY17Q1:Windows10PerUserFY19Q4)
windowsclient <- gather(windowsclient, ym, WinClient, WindowsClientFY17Q1:WindowsClientFY19Q4)
windowse5 <- gather(windowse5, ym, WinE5, WindowsE5FY17Q1:WindowsE5FY19Q4)

win10perdevice <- preprocessDate(win10perdevice) %>% select(TPID, ym1, Win10PerDevice)
win10peruser <- preprocessDate(win10peruser) %>% select(TPID, ym1, Win10PerUser)
windowsclient <- preprocessDate(windowsclient) %>% select(TPID, ym1, WinClient)
windowse5 <- preprocessDate(windowse5) %>% select(TPID, ym1, WinE5)

windows <- full_join(win10perdevice, win10peruser, by = c("TPID", "ym1")) %>% 
  full_join(., windowsclient, by = c("TPID", "ym1")) %>% 
  full_join(., windowse5, by = c("TPID", "ym1"))

windows[is.na(windows)] <- 0

write.csv(windows, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\Processed\\WindowsSeats.csv", row.names = FALSE)

