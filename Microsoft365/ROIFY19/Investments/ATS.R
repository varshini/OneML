# Script to calculate ATS features - # of months of touch, average of time allocated #

# will keep same resourcing as Mar 19 for earlier - repeat this
# see how many months of continuous touch each tpid has had 
# Also calculate average amount of time allocated to that tpid through those months 

ats_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\ATS.csv")
colnames(ats_data)[1] <- "TPID"

# Taking all the assignments as of March 2019 to the rest of FY19
ats_data$Feb2019 <- ats_data$Mar2019
ats_data$Jan2019 <- ats_data$Mar2019
ats_data$Dec2018 <- ats_data$Mar2019
ats_data$Nov2018 <- ats_data$Mar2019
ats_data$Oct2018 <- ats_data$Mar2019
ats_data$Sep2018 <- ats_data$Mar2019
ats_data$Aug2018 <- ats_data$Mar2019
ats_data$Jul2018 <- ats_data$Mar2019

ats_data_long <- gather(ats_data, Month, Touch, Mar2019:Jul2018) 

ats_data_long$Month <- case_when(
  ats_data_long$Month == "Jul2018" ~ "2018/07/31",
  ats_data_long$Month == "Aug2018" ~ "2018/08/31",
  ats_data_long$Month == "Sep2018" ~ "2018/09/30",
  ats_data_long$Month == "Oct2018" ~ "2018/10/31",
  ats_data_long$Month == "Nov2018" ~ "2018/11/30",
  ats_data_long$Month == "Dec2018" ~ "2018/12/31",
  ats_data_long$Month == "Jan2019" ~ "2019/01/31",
  ats_data_long$Month == "Feb2019" ~ "2019/02/28",
  ats_data_long$Month == "Mar2019" ~ "2019/03/31",
  ats_data_long$Month == "Apr2019" ~ "2019/04/30",
  ats_data_long$Month == "May2019" ~ "2019/05/31",
  ats_data_long$Month == "Jun2019" ~ "2019/06/30",
  ats_data_long$Month == "Jul2019" ~ "2019/07/31",
  ats_data_long$Month == "Aug2019" ~ "2019/08/31",
  ats_data_long$Month == "Sep2019" ~ "2019/09/30",
  ats_data_long$Month == "Oct2019" ~ "2019/10/31"
)
  
ats_data_long$Month <- as.Date(ymd(ats_data_long$Month))
ats_data_long <- ats_data_long %>% filter(Touch > 0)

# calculate # of months of touch and avg allocation time 

calculateMonths <- function(data, start_date, end_date, monthvar, allocvar)
{
  data <- filter(data, Month >= as.Date(start_date) & Month <= as.Date(end_date))
  data <- data %>% group_by(TPID) %>% summarise(NoOfMonths = n(), 
                                           AvgAllocatedTime = mean(Touch))
  data[monthvar] <- data$NoOfMonths
  data[allocvar] <- data$AvgAllocatedTime
  return ((data[,c(1,4,5)]) )
  #return(data)
}

jul1819 <- calculateMonths(ats_data_long, "2018-08-01", "2019-05-31", "ATSNoOfMonthsJul", "ATSAvgAllocatedTimeJuly")
aug1819 <- calculateMonths(ats_data_long, "2018-09-01", "2019-06-30", "ATSNoOfMonthsAug", "ATSAvgAllocatedTimeAug")
sep1819 <- calculateMonths(ats_data_long, "2018-10-01", "2019-07-31", "ATSNoOfMonthsSep", "ATSAvgAllocatedTimeSep")
oct1819 <- calculateMonths(ats_data_long, "2018-11-01", "2019-08-31", "ATSNoOfMonthsOct", "ATSAvgAllocatedTimeOct")
nov1819 <- calculateMonths(ats_data_long, "2018-12-01", "2019-09-30", "ATSNoOfMonthsNov", "ATSAvgAllocatedTimeNov")
dec1819 <- calculateMonths(ats_data_long, "2019-01-01", "2019-10-31", "ATSNoOfMonthsDec", "ATSAvgAllocatedTimeDec")

final_data <- full_join(jul1819, aug1819, by = "TPID") %>% 
  full_join(., sep1819, by = "TPID") %>% 
  full_join(., oct1819, by = "TPID") %>% 
  full_join(., nov1819, by = "TPID") %>% 
  full_join(., dec1819, by = "TPID")

final_data[is.na(final_data)] <- 0
write.csv(final_data, "C:\\Users\\varamase\\Documents\\DataStreams\\ROIFY19\\Investments\\Features\\ATS.csv", row.names = FALSE)  

  
