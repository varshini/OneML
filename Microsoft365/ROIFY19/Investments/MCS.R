### Processing Services data  ### 

library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(readxl)
library("reshape2")
options(stringsAsFactors = FALSE)

### Services - MCS ###

elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

mcs <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ServicesMCS.xlsx")
colnames(mcs)[4] <- "ProjectStartDate"
colnames(mcs)[5] <- "ProjectEndDate"
colnames(mcs)[13] <- "ProjectSize"

######## overall - repeat for teams and core apps #############
#all_wkl <- mcs %>% filter(`Overall O365 MAU Growth` == "Yes" | `Overall O365 MAU Growth` == "Office 365")
#core_wkl <- mcs %>% filter(`Core Workloads MAU Growth` == "Yes" | `Overall O365 MAU Growth` == "Office 365")
modern_comms <-  mcs %>% filter(`Teams` == "Yes" | `Overall O365 MAU Growth` == "Office 365")

data <- modern_comms # CHANGE FOR DIFFERENT WORKLOADS

# Convert to month flags for distinct months of touch

data$Aug2018 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2018-08-01"), ymd("2018-08-31"))), 1, 0)
data$Sep2018 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2018-09-01"), ymd("2018-09-30"))), 1, 0)
data$Oct2018 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2018-10-01"), ymd("2018-10-31"))), 1, 0)
data$Nov2018 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2018-11-01"), ymd("2018-11-30"))), 1, 0)
data$Dec2018 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2018-12-01"), ymd("2018-12-31"))), 1, 0)
data$Jan2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-01-01"), ymd("2019-01-31"))), 1, 0)
data$Feb2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-02-01"), ymd("2019-02-28"))), 1, 0)
data$Mar2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-03-01"), ymd("2019-03-31"))), 1, 0)
data$Apr2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-04-01"), ymd("2019-04-30"))), 1, 0)
data$May2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-05-01"), ymd("2019-05-31"))), 1, 0)
data$Jun2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-06-01"), ymd("2019-06-30"))), 1, 0)
data$Jul2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-07-01"), ymd("2019-07-31"))), 1, 0)
data$Aug2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-08-01"), ymd("2019-08-31"))), 1, 0)
data$Sep2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-09-01"), ymd("2019-09-30"))), 1, 0)
data$Oct2019 <- ifelse(int_overlaps(interval(ymd(data$ProjectStartDate), data$ProjectEndDate), interval(ymd("2019-10-01"), ymd("2019-10-31"))), 1, 0)


data <- data %>% group_by(TPID,ProjectSize) %>% summarise(Aug2018 = max(Aug2018), Sep2018 = max(Sep2018), Oct2018 = max(Oct2018),Nov2018 = max(Nov2018),Dec2018 = max(Dec2018),
                                                          Jan2019 = max(Jan2019),Feb2019 = max(Feb2019),Mar2019 = max(Mar2019),Apr2019 = max(Apr2019),May2019 = max(May2019),
                                                          Jun2019 = max(Jun2019),Jul2019 = max(Jul2019),Aug2019 = max(Aug2019),Sep2019 = max(Sep2019),Oct2019 = max(Oct2019))

data$JulMonths <- data$Aug2018 + data$Sep2018 + data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019
data$AugMonths <- data$Sep2018 + data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019
data$SepMonths <- data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019
data$OctMonths <- data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019
data$NovMonths <- data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019 + data$Sep2019
data$DecMonths <- data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019 + data$Sep2019 + data$Oct2019
data <- data %>% select(TPID,ProjectSize, JulMonths, AugMonths, SepMonths, OctMonths, NovMonths, DecMonths)

a <- gather(data, MonthYear, MCSMonths, JulMonths:DecMonths)
data1 <- spread(a, ProjectSize, MCSMonths)
colnames(data1) <- paste("MCSMonths",colnames(data1), sep = "")
colnames(data1)[1] <- "TPID"
colnames(data1)[2] <- "ym"
  
data1[is.na(data1)] <- 0
write.csv(data1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\MCS.csv", row.names = FALSE)  
