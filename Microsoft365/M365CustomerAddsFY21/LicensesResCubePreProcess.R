library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)

options(stringsAsFactors = FALSE)

smctpids <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smctpids)[1] <- "FinalTPID"
smctpids <- filter(smctpids, FY21.Subsegment %in% c("SM&C Government - Corporate", "Enterprise Growth", "SM&C Commercial - Corporate") ) #27980 Tpids
smctpids$FinalTPID <- as.character(smctpids$FinalTPID)

#######M365 licenses##########
m365data <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\RescubeM365Licenses.xlsx", sheet = "Sheet1")
m365data$FinalTPID <- as.character(m365data$FinalTPID)
m365data <- inner_join(smctpids, m365data, by = "FinalTPID")
m365data[is.na(m365data)] <- 0
m365data <- gather(m365data, Rev_Sum_Category, SeatCount, `O365 - M365 Business`:`O365 E5 - M365 E5 FUSL`)
m365data <- m365data %>% select(FinalTPID, Rev_Sum_Category, `Fiscal Month`, SeatCount)

#######O365licenses###########
o365data_entsmc <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\RescubeLicensesProc.xlsx", sheet = "O365M365EntSMC")
colnames(o365data_entsmc)[4] <- "SeatCount"
o365data_entsmc <- o365data_entsmc %>% filter(Rev_Sum_Category %in% c("O365 Plan E1/E2" , "O365 Plan E3", "O365 Plan E5", "O365 Plan E5 Cloud Add-On", 
                                                                      "O365 Plan E1/E2 Cloud Add-On", "O365 Plan E3 Cloud Add-On", "O365 Plan E4", "O365 Plan E4 Cloud Add-On"))
o365data_entsmc <- o365data_entsmc %>% select(FinalTPID,  `Fiscal Month`, Rev_Sum_Category, SeatCount)

o365data_smb1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Sep2018")
o365data_smb1$`Fiscal Month` <- "September, 2018"
o365data_smb1 <- gather(o365data_smb1, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Dec2018")
o365data_smb2$`Fiscal Month` <- "December, 2018"
o365data_smb2 <- gather(o365data_smb2, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Mar2019")
o365data_smb3$`Fiscal Month` <- "March, 2019"
o365data_smb3 <- gather(o365data_smb3, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Jun2019")
o365data_smb4$`Fiscal Month` <- "June, 2019"
o365data_smb4 <- gather(o365data_smb4, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Sep2019")
o365data_smb5$`Fiscal Month` <- "September, 2019"
o365data_smb5 <- gather(o365data_smb5, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Dec2019")
o365data_smb6$`Fiscal Month` <- "December, 2019"
o365data_smb6 <- gather(o365data_smb6, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Mar2020")
o365data_smb7$`Fiscal Month` <- "March, 2020"
o365data_smb7 <- gather(o365data_smb7, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data_smb8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeatsOM.xlsx", sheet = "Jun2020")
o365data_smb8$`Fiscal Month` <- "June, 2020"
o365data_smb8 <- gather(o365data_smb8, Rev_Sum_Category, SeatCount, `O365 Plan E1/E2`:`O365 Plan E5 Cloud Add-On`)

o365data <- bind_rows(o365data_entsmc, o365data_smb1, o365data_smb2, o365data_smb3, o365data_smb4, o365data_smb5, o365data_smb6, o365data_smb7, o365data_smb8)
o365data <- inner_join(smctpids, o365data, by = "FinalTPID")
o365data <- o365data %>% select(FinalTPID, Rev_Sum_Category, `Fiscal Month`, SeatCount)

#########Others#########
other_entsmc <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\RescubeLicensesProc.xlsx", sheet = "OtherEntSMC")
colnames(other_entsmc)[4] <- "SeatCount"
other_entsmc <- other_entsmc %>% select(FinalTPID,  `Fiscal Month`, Rev_Sum_Category, SeatCount)

o365data_smb1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersSep2018")
o365data_smb1$`Fiscal Month` <- "September, 2018"
o365data_smb1 <- gather(o365data_smb1, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersDec2018")
o365data_smb2$`Fiscal Month` <- "December, 2018"
o365data_smb2 <- gather(o365data_smb2, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb3 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersMar2019")
o365data_smb3$`Fiscal Month` <- "March, 2019"
o365data_smb3 <- gather(o365data_smb3, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb4 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersJun2019")
o365data_smb4$`Fiscal Month` <- "June, 2019"
o365data_smb4 <- gather(o365data_smb4, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb5 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersSep2019")
o365data_smb5$`Fiscal Month` <- "September, 2019"
o365data_smb5 <- gather(o365data_smb5, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb6 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersDec2019")
o365data_smb6$`Fiscal Month` <- "December, 2019"
o365data_smb6 <- gather(o365data_smb6, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb7 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersMar2020")
o365data_smb7$`Fiscal Month` <- "March, 2020"
o365data_smb7 <- gather(o365data_smb7, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

o365data_smb8 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\SMBSeats.xlsx", sheet = "OthersJun2020")
o365data_smb8$`Fiscal Month` <- "June, 2020"
o365data_smb8 <- gather(o365data_smb8, Rev_Sum_Category, SeatCount, `Advanced Compliance`:`Power BI Suites - M365`)

other_data <- bind_rows(other_entsmc, o365data_smb1, o365data_smb2, o365data_smb3, o365data_smb4, o365data_smb5, o365data_smb6, o365data_smb7, o365data_smb8)
other_data <- inner_join(smctpids, other_data, by = "FinalTPID")
other_data <- other_data %>% select(FinalTPID, Rev_Sum_Category, `Fiscal Month`, SeatCount)
#################

all_licenses <- rbind(m365data, o365data, other_data)
all_licenses$Date <- case_when(all_licenses$`Fiscal Month` == "September, 2018" ~ "9/30/2018 12:00:00 AM", 
                               all_licenses$`Fiscal Month` == "December, 2018" ~ "12/31/2018 12:00:00 AM", 
                               all_licenses$`Fiscal Month` == "March, 2019" ~ "3/31/2019 12:00:00 AM", 
                               all_licenses$`Fiscal Month` == "June, 2019" ~ "6/30/2019 12:00:00 AM", 
                               all_licenses$`Fiscal Month` == "September, 2019" ~ "9/30/2019 12:00:00 AM", 
                               all_licenses$`Fiscal Month` == "December, 2019" ~ "12/31/2019 12:00:00 AM", 
                               all_licenses$`Fiscal Month` == "March, 2020" ~ "3/31/2020 12:00:00 AM",
                               all_licenses$`Fiscal Month` == "June, 2020" ~ "6/30/2020 12:00:00 AM"
                               )

all_licenses <- all_licenses %>% select(FinalTPID, Rev_Sum_Category, Date, SeatCount)
all_licenses[is.na(all_licenses)] <- 0
write.csv(all_licenses, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\ResCubeSeatsData.csv", row.names = FALSE)


