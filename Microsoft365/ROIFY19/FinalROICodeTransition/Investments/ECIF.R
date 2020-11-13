# Script to calculate ECIF features based on data from Ming Jun #
library(readxl)
library(dplyr)
library(tidyr)
library(zoo)
options(stringsAsFactors = FALSE)


ecif_19 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ECIFDataProcessed.xlsx", sheet = "FY19")
ecif_20 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ECIFDataProcessed.xlsx", sheet = "FY20")

ecif_data <- full_join(ecif_19, ecif_20, by="TPID")
ecif_data[is.na(ecif_data)] <- 0
ecif_data <- ecif_data %>% group_by(TPID) %>% summarise(Jul18 = max(Jul18), Aug18 = max(Aug18),
                                                        Sep18 = max(Sep18), Oct18 = max(Oct18), Nov18 = max(Nov18), Dec18 = max(Dec18),
                                                        Jan19 = max(Jan19), Feb19 = max(Feb19), Mar19 = max(Mar19), Apr19 = max(Apr19),
                                                        May19 = max(May19), Jun19 = max(Jun19), Jul19 = max(Jul19),Aug19 = max(Aug19),
                                                        Sep19 = max(Sep19),Oct19 = max(Oct19),Nov19 = max(Nov19),Dec19 = max(Dec19))

names(ecif_data) <- c("TPID","Jul 2018","Aug 2018","Sep 2018","Oct 2018", "Nov 2018", "Dec 2018","Jan 2019",
                      "Feb 2019","Mar 2019", "Apr 2019","May 2019","Jun 2019", "Jul 2019","Aug 2019","Sep 2019",
                      "Oct 2019", "Nov 2019", "Dec 2019")

ecif_data_1 <- gather(ecif_data, Month, Touched, `Jul 2018`:`Dec 2019`)
ecif_data_1$Month <- as.Date(as.yearmon(ecif_data_1$Month))                                       

calculateMonths <- function(data, start_date, end_date, monthvar)
{
  data <- filter(data, data$Month >= start_date & data$Month <= end_date)
  data <- data %>% group_by(TPID) %>% summarize(NoOfMonths = sum(Touched))
  data[monthvar] <- data$NoOfMonths
  return (data[,c(1,3)])
}


ecif_data_Jul <- calculateMonths(ecif_data_1,"2018-08-01", "2019-05-31", "ECIFJulMonths")
ecif_data_Aug <- calculateMonths(ecif_data_1,"2018-09-01", "2019-06-30", "ECIFAugMonths")
ecif_data_Sep <- calculateMonths(ecif_data_1,"2018-10-01", "2019-07-31", "ECIFSepMonths")
ecif_data_Oct <- calculateMonths(ecif_data_1,"2018-11-01", "2019-08-31", "ECIFOctMonths")
ecif_data_Nov <- calculateMonths(ecif_data_1,"2018-12-01", "2019-09-30", "ECIFNovMonths")
ecif_data_Dec <- calculateMonths(ecif_data_1,"2019-01-01", "2019-10-31", "ECIFDecMonths")

ecif_data_2 <- full_join(ecif_data_Jul, ecif_data_Aug, by = "TPID") %>%
  full_join(., ecif_data_Sep, by = "TPID") %>%
  full_join(., ecif_data_Oct, by = "TPID") %>%
  full_join(., ecif_data_Nov, by = "TPID") %>%
  full_join(., ecif_data_Dec, by = "TPID")

ecif_data_2[is.na(ecif_data_2)] <- 0
write.csv(ecif_data_2, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ECIF.csv", row.names = FALSE) 


