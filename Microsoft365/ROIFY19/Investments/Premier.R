### Processing Premier data  ### 

library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(readxl)
options(stringsAsFactors = FALSE)

premier <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Premier.csv")
colnames(premier)[1] <- "TPID"

all_wkl <- premier %>% filter(Overall.Workloads.MAU.Growth == "Office 365")
# No core workloads covered
modern_comms <-  premier %>% filter(`Teams` == "Yes" | Overall.Workloads.MAU.Growth == "Office 365")

## Calculate Features ##

data <- modern_comms

# map to months of work #
data$Month <- case_when(data$MonthName == "01-Jul" & data$FiscalYear == 2019 ~ "Jul2018",
                        data$MonthName == "02-Aug" & data$FiscalYear == 2019 ~ "Aug2018",
                        data$MonthName == "03-Sep" & data$FiscalYear == 2019 ~ "Sep2018",
                        data$MonthName == "04-Oct" & data$FiscalYear == 2019 ~ "Oct2018",
                        data$MonthName == "05-Nov" & data$FiscalYear == 2019 ~ "Nov2018",
                        data$MonthName == "06-Dec" & data$FiscalYear == 2019 ~ "Dec2018",
                        data$MonthName == "07-Jan" & data$FiscalYear == 2019 ~ "Jan2019",
                        data$MonthName == "08-Feb" & data$FiscalYear == 2019 ~ "Feb2019",
                        data$MonthName == "09-Mar" & data$FiscalYear == 2019 ~ "Mar2019",
                        data$MonthName == "10-Apr" & data$FiscalYear == 2019 ~ "Apr2019",
                        data$MonthName == "11-May" & data$FiscalYear == 2019 ~ "May2019",
                        data$MonthName == "12-Jun" & data$FiscalYear == 2019 ~ "Jun2019",
                        data$MonthName == "01-Jul" & data$FiscalYear == 2020 ~ "Jul2019",
                        data$MonthName == "02-Aug" & data$FiscalYear == 2020 ~ "Aug2019",
                        data$MonthName == "03-Sep" & data$FiscalYear == 2020 ~ "Sep2019",
                        data$MonthName == "04-Oct" & data$FiscalYear == 2020 ~ "Oct2019",
                        data$MonthName == "05-Nov" & data$FiscalYear == 2020 ~ "Nov2019",
                        data$MonthName == "06-Dec" & data$FiscalYear == 2020 ~ "Dec2019")

data <- data %>% select(TPID, Month)
data$Active <- 1
data <- data %>% group_by(TPID, Month) %>% summarise(Active = max(Active))
data <- spread(data, Month, Active)
data[is.na(data)] <- 0

data$JulMonths <- data$Aug2018 + data$Sep2018 + data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019
data$AugMonths <- data$Sep2018 + data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019
data$SepMonths <- data$Oct2018 + data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019
data$OctMonths <- data$Nov2018 + data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019
data$NovMonths <- data$Dec2018 + data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019 + data$Sep2019
data$DecMonths <- data$Jan2019 + data$Feb2019 + data$Mar2019 + data$Apr2019 + data$May2019 + data$Jun2019 + data$Jul2019 + data$Aug2019 + data$Sep2019 + data$Oct2019
data <- data %>% select(TPID, JulMonths, AugMonths, SepMonths, OctMonths, NovMonths, DecMonths)

data1 <- gather(data, MonthYear, PremierMonths, JulMonths:DecMonths)
colnames(data1)[2] <- "ym"

write.csv(data1, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ModernComms\\Premier.csv", row.names = FALSE)  
