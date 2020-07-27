library(readxl)
library(dplyr)
library(tidyverse)
options(stringsAsFactors = FALSE)

smctpids <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smctpids)[1] <- "FinalTPID"
smctpids <- filter(smctpids, FY21.Subsegment %in% c("SM&C Government - Corporate", "Enterprise Growth", "SM&C Commercial - Corporate") ) #27980 Tpids

#licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\AllLicensesByRevSumCat.csv")
licenses <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\RawData\\Seats\\ResCubeSeatsData.csv")
licenses <- inner_join(licenses, smctpids, by = "FinalTPID")
licenses <- licenses %>% filter(Rev_Sum_Category %in% c("O365 - M365 E3 FUSL", "O365 - M365 Business", "O365 - M365 E3 CAO", "O365 - M365 F0", 
                                                        "O365 - M365 F1", "O365 E5 - M365 E5 CAO", "O365 E5 - M365 E5 FUSL"))

licenses1 <- licenses %>% filter(Date %in% c("6/30/2020 12:00:00 AM"))
licenses_wide <- spread(licenses1, Rev_Sum_Category, SeatCount)
licenses_wide[is.na(licenses_wide)] <- 0                                               
licenses_wide$M365E3May2020 <- licenses_wide$`O365 - M365 Business` + licenses_wide$`O365 - M365 E3 CAO` + licenses_wide$`O365 - M365 E3 FUSL`
licenses_wide$M365E5May2020 <- licenses_wide$`O365 E5 - M365 E5 CAO` + licenses_wide$`O365 E5 - M365 E5 FUSL`
licenses_wide$M365F1May2020 <- licenses_wide$`O365 - M365 F1`
licenses_wide$M365May2020 <- licenses_wide$M365F1May2020 + licenses_wide$M365E3May2020 + licenses_wide$M365E5May2020
licenses_wide <- licenses_wide %>% select(FinalTPID, M365F1May2020, M365E3May2020, M365E5May2020, M365May2020)

licenses2 <- licenses %>% filter(Date %in% c("12/31/2019 12:00:00 AM"))
licenses_wide1 <- spread(licenses2, Rev_Sum_Category, SeatCount)
licenses_wide1[is.na(licenses_wide1)] <- 0                                               
licenses_wide1$M365E3Dec2019 <- licenses_wide1$`O365 - M365 Business` + licenses_wide1$`O365 - M365 E3 CAO` + licenses_wide1$`O365 - M365 E3 FUSL`
licenses_wide1$M365E5Dec2019 <- licenses_wide1$`O365 E5 - M365 E5 CAO` + licenses_wide1$`O365 E5 - M365 E5 FUSL`
licenses_wide1$M365F1Dec2019 <- licenses_wide1$`O365 - M365 F1`
licenses_wide1$M365Dec2019 <- licenses_wide1$M365F1Dec2019 + licenses_wide1$M365E3Dec2019 + licenses_wide1$M365E5Dec2019
licenses_wide1 <- licenses_wide1 %>% select(FinalTPID, M365F1Dec2019, M365E3Dec2019, M365E5Dec2019, M365Dec2019)

licenses3 <- licenses %>% filter(Date %in% c("9/30/2019 12:00:00 AM"))
licenses_wide2 <- spread(licenses3, Rev_Sum_Category, SeatCount)
licenses_wide2[is.na(licenses_wide2)] <- 0                                               
licenses_wide2$M365E3Sep2019 <- licenses_wide2$`O365 - M365 Business` + licenses_wide2$`O365 - M365 E3 CAO` + licenses_wide2$`O365 - M365 E3 FUSL`
licenses_wide2$M365E5Sep2019 <- licenses_wide2$`O365 E5 - M365 E5 CAO` + licenses_wide2$`O365 E5 - M365 E5 FUSL`
licenses_wide2$M365F1Sep2019 <- licenses_wide2$`O365 - M365 F1`
licenses_wide2$M365Sep2019 <- licenses_wide2$M365F1Sep2019 + licenses_wide2$M365E3Sep2019 + licenses_wide2$M365E5Sep2019
licenses_wide2 <- licenses_wide2 %>% select(FinalTPID, M365F1Sep2019, M365E3Sep2019, M365E5Sep2019, M365Sep2019)

licenses_wide3 <- full_join(licenses_wide, licenses_wide1, by = "FinalTPID")
licenses_wide3[is.na(licenses_wide3)] <- 0

nrow(filter(licenses_wide3, M365Sep2019 < 300 & M365May2020 >= 300)) #1493 - Q2 to Q4

nrow(filter(licenses_wide3, M365Dec2019 < 300 & M365May2020 >= 300)) #999 - Q3 and Q4
nrow(filter(licenses_wide3, M365E5May2020 > 300)) # to match numbers with Portal

# conversion tpids - # TRAINING LABELS # 

#Customer Adds and Expansion
conversions <- filter(licenses_wide3, (M365Dec2019 < 300 & M365May2020 >= 300) | (M365May2020 - M365Dec2019 >= 300)) %>%
                select(FinalTPID)
conversions$M365Label <- 1
non_conversions <- data.frame(setdiff(smctpids$FinalTPID, conversions$FinalTPID))
colnames(non_conversions)[1] <- "FinalTPID"
non_conversions$M365Label <- 0

#Only Customer Adds
conversions <- filter(licenses_wide3, (M365Dec2019 < 300 & M365May2020 >= 300)) %>%
  select(FinalTPID)
conversions$M365Label <- 1
exclusions <- filter(licenses_wide3, M365Dec2019 >= 300) %>% select(FinalTPID)
non_conversions_final <- data.frame(setdiff(smctpids$FinalTPID, exclusions$FinalTPID))
colnames(non_conversions_final)[1] <- "FinalTPID"
non_conversions_final <- data.frame(setdiff(non_conversions_final$FinalTPID, conversions$FinalTPID))
colnames(non_conversions_final)[1] <- "FinalTPID"
non_conversions_final$M365Label <- 0

training_labels <- rbind(non_conversions, conversions)
write.csv(training_labels, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\FY21TrainingLabelsFinal.csv", row.names = FALSE)

