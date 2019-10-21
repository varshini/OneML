#  Create labels of M365 adds #

library(readxl)
library(dplyr)
library(tidyr)

m365 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\RawData\\Seats\\M365Seats.xlsx")
m365[is.na(m365)] <- 0

m365$E3FY18Q2 <- m365$E3EnterpriseFY18Q2 + m365$BusinessSeatsFY18Q2 + m365$EDUSeatsFY18Q2
m365$E3FY18Q3 <- m365$E3EnterpriseFY18Q3 + m365$BusinessSeatsFY18Q3 + m365$EDUSeatsFY18Q3
m365$E3FY18Q4 <- m365$E3EnterpriseFY18Q4 + m365$BusinessSeatsFY18Q4 + m365$EDUSeatsFY18Q4
m365$E3FY19Q1 <- m365$E3EnterpriseFY19Q1 + m365$BusinessSeatsFY19Q1 + m365$EDUSeatsFY19Q1
m365$E3FY19Q2 <- m365$E3EnterpriseFY19Q2 + m365$BusinessSeatsFY19Q2 + m365$EDUSeatsFY19Q2
m365$E3FY19Q3 <- m365$E3EnterpriseFY19Q3 + m365$BusinessSeatsFY19Q3 + m365$EDUSeatsFY19Q3
m365$E3FY19Q4 <- m365$E3EnterpriseFY19Q4 + m365$BusinessSeatsFY19Q4 + m365$EDUSeatsFY19Q4

m365$m365totalseatsFY18Q2 <- m365$F1SeatsFY18Q2 + m365$E3FY18Q2 + m365$E5EnterpriseFY18Q2 
m365$m365totalseatsFY18Q3 <- m365$F1SeatsFY18Q3 + m365$E3FY18Q3 + m365$E5EnterpriseFY18Q3 
m365$m365totalseatsFY18Q4 <- m365$F1SeatsFY18Q4 + m365$E3FY18Q4 + m365$E5EnterpriseFY18Q4 
m365$m365totalseatsFY19Q1 <- m365$F1SeatsFY19Q1 + m365$E3FY19Q1 + m365$E5EnterpriseFY19Q1 
m365$m365totalseatsFY19Q2 <- m365$F1SeatsFY19Q2 + m365$E3FY19Q2 + m365$E5EnterpriseFY19Q2 
m365$m365totalseatsFY19Q3 <- m365$F1SeatsFY19Q3 + m365$E3FY19Q3 + m365$E5EnterpriseFY19Q3 
m365$m365totalseatsFY19Q4 <- m365$F1SeatsFY19Q4 + m365$E3FY19Q4 + m365$E5EnterpriseFY19Q4 

m365$m365CustomerAddFY18Q2 <- ifelse(m365$m365totalseatsFY18Q2 > 275, 1, 0) # 275 to keep a flexible threshold 
m365$m365CustomerAddFY18Q3 <- ifelse(m365$m365totalseatsFY18Q3 > 275, 1, 0)
m365$m365CustomerAddFY18Q4 <- ifelse(m365$m365totalseatsFY18Q4 > 275, 1, 0)
m365$m365CustomerAddFY19Q1 <- ifelse(m365$m365totalseatsFY19Q1 > 275, 1, 0)
m365$m365CustomerAddFY19Q2 <- ifelse(m365$m365totalseatsFY19Q2 > 275, 1, 0)
m365$m365CustomerAddFY19Q3 <- ifelse(m365$m365totalseatsFY19Q3 > 275, 1, 0)
m365$m365CustomerAddFY19Q4 <- ifelse(m365$m365totalseatsFY19Q4 > 275, 1, 0)

m365$TPID <- as.numeric(m365$TPID)

fy20mal <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\FY20MAL.xlsx")
smc <- fy20mal %>% filter(SegmentGroup == "SMC Managed")
colnames(smc)[1] <- "TPID"
smc$TPID <- as.numeric(smc$TPID)

m365_join <- left_join(smc, m365, by = "TPID") 
m365_join[is.na(m365_join)] <- 0 

#Labels# - get for FY19Q3 and FY19Q4 as seperate quarters so you can provide a gap of a quarter, add ym as a column for joining to features  

# Label 1
q3_adds <- filter(m365_join, m365CustomerAddFY18Q2 == 0 & m365CustomerAddFY18Q3 == 0 & 
              m365CustomerAddFY18Q4 == 0 & m365CustomerAddFY19Q1 == 0 & m365CustomerAddFY19Q2 == 0 
            & m365CustomerAddFY19Q3 == 1)
q3_adds$CustomerAdd <- 1

q4_adds <- filter(m365_join, m365CustomerAddFY18Q2 == 0 & m365CustomerAddFY18Q3 == 0 & 
              m365CustomerAddFY18Q4 == 0 & m365CustomerAddFY19Q1 == 0 & m365CustomerAddFY19Q2 == 0 
            & m365CustomerAddFY19Q3 == 0 & m365CustomerAddFY19Q4 == 1)
q4_adds$CustomerAdd <- 1

# Label 0
q3_negatives <- filter(m365_join, m365CustomerAddFY18Q2 == 0 & m365CustomerAddFY18Q3 == 0 & 
              m365CustomerAddFY18Q4 == 0 & m365CustomerAddFY19Q1 == 0 & m365CustomerAddFY19Q2 == 0 
            & m365CustomerAddFY19Q3 == 0)
q3_negatives$CustomerAdd <- 0

q4_negatives <- filter(m365_join, m365CustomerAddFY18Q2 == 0 & m365CustomerAddFY18Q3 == 0 & 
              m365CustomerAddFY18Q4 == 0 & m365CustomerAddFY19Q1 == 0 & m365CustomerAddFY19Q2 == 0 
            & m365CustomerAddFY19Q3 == 0 & m365CustomerAddFY19Q4 == 0)
q4_negatives$CustomerAdd <- 0

q3_data <- rbind(q3_adds, q3_negatives)
q3_data$ym <- "FY19Q3"

q4_data <- rbind(q4_adds, q4_negatives)
q4_data$ym <- "FY19Q4"

labels <- rbind(q3_data, q4_data)

labels_only <- labels %>% select(TPID, CustomerAdd, ym)

write.csv(labels_only, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\M365Labels.csv", row.names = FALSE)
write.csv(labels, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\M365LabelsExpanded.csv", row.names = FALSE)


# convert two qaurter labels into 1 #
labels <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\M365Labels.csv")
ca_pos <- filter(labels, CustomerAdd == 1) %>% dplyr::select(TPID, CustomerAdd)
ca_neg <- filter(labels, CustomerAdd == 0) %>% dplyr::select(TPID)
ca_neg <- data.frame(unique(ca_neg$TPID))
colnames(ca_neg)[1] <- "TPID"
ca_neg$CustomerAdd <- 0
labels <- rbind(ca_pos, ca_neg)
labels <- labels %>% group_by(TPID) %>% summarise(CustomerAdd = max(CustomerAdd))

write.csv(labels, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\Data\\M365Labels6months.csv", row.names = FALSE)

