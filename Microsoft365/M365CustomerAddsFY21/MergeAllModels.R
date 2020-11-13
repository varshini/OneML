library (dplyr)
library(tidyr)
options(stringsAsFactors = FALSE)

o365 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\TPID_propensities_SMC.csv", sep = ";")
colnames(o365) <- c("RowNo", "FinalTPID", "Recommendation", "Propensity", "WhyRecommended", "Conversation")
o365$Model <- "O365"
o365$Recommendation <- ifelse(grepl("High", o365$Recommendation) == TRUE, "This customer has a high probability to purchase 300+ O365 seats in the next year", 
                               ifelse(grepl("Medium", o365$Recommendation) == TRUE, "This customer has a medium probability to purchase 300+ O365 seats in the next year", "NA"))
o365$Propensity <- as.integer(o365$Propensity)
o365$WhyRecommended <- paste(o365$WhyRecommended, '.', sep = '')
o365 <- o365 %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity, Model)

ems <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\EMSE5SMC_reasons_capped_removed_v2.csv", sep = ";")
colnames(ems) <- c("FinalTPID", "WhyRecommended", "Recommendation", "Propensity", "Conversation")
ems$Recommendation <- ifelse(grepl("high", ems$Recommendation) == TRUE, "This customer has a high probability to purchase 300+ EMS E5 seats in the next year", 
                              ifelse(grepl("medium", ems$Recommendation) == TRUE, "This customer has a medium probability to purchase 300+ EMS E5 seats in the next year", "NA"))
ems$Propensity <- as.integer(ems$Propensity)
ems$WhyRecommended <- paste(ems$WhyRecommended, '.', sep = '')
ems <- ems %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity)
ems$Model <- "EMSE5"

ca <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalDROutputv2.csv") 
ca$Recommendation <- ifelse(grepl("High", ca$Recommendation) == TRUE, "This customer has a high probability to purchase 300+ M365 seats in the next 6 months", 
                             ifelse(grepl("Medium", ca$Recommendation) == TRUE, "This customer has a medium probability to purchase 300+ M365 seats in the next 6 months", "NA"))
ca$Propensity <- ca$Propensity * 100
ca$Propensity <- as.integer(ca$Propensity)
ca$WhyRecommended <- paste(ca$WhyRecommended, '.', sep = '')
ca <- ca %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity)
ca$Model <- "CA"

e5 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutput_ME5_63020_non1A1B.csv")
e5$Recommendation <- ifelse(grepl("high", e5$Recommendation) == TRUE, "This customer has a high probability to purchase 300+ M365 E5 seats in the next 6 months", 
                            ifelse(grepl("medium", e5$Recommendation) == TRUE, "This customer has a medium probability to purchase 300+ M365 E5 seats in the next 6 months", "NA"))
e5$Propensity <- as.integer(e5$Propensity)
e5$WhyRecommended <- paste(e5$WhyRecommended, '.', sep = '')
e5 <- e5 %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity)
e5$Model <- "E5"

all_data <- rbind(o365, ems, ca, e5)
all_data$Flag <- 1

all_data <- spread(all_data, Model, Flag)
all_data[is.na(all_data)] <- 0

#### Prioritization Logic Analysis ####
all_data <- all_data %>% group_by(FinalTPID) %>% summarise(O365 = max(O365), E5 = max(E5),CA = max(CA),EMSE5 = max(EMSE5))

# all_data$RecommendationType <- case_when(all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 0 & all_data$O365 == 0 ~ "E5", 
#                                          all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 0 ~ "CA",
#                                          all_data$E5 == 0 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "EMS",
#                                          all_data$E5 == 0 & all_data$CA == 0 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "O365",
#                                          
#                                          all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "E5O365",
#                                          all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "E5EMSO365",
#                                          all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 0 ~ "E5CA",
#                                          all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "E5CAO365",
#                                          all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "E5CAEMS",
#                                          all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "E5EMS",
#                                          all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "E5CAEMSO365",
#                                          
#                                          all_data$E5 == 0 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "EMSO365",
#                                          all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "CAO365",
#                                          all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "CAEMS",
#                                          all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "CAEMSO365")
                                            
all_data$RecommendationType <- case_when(all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 0 & all_data$O365 == 0 ~ "E5", 
                                         all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 0 ~ "CA",
                                         all_data$E5 == 0 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "EMS",
                                         all_data$E5 == 0 & all_data$CA == 0 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "O365",
                                         
                                         all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "E5O365",
                                         all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "E5EMS",
                                         all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 0 ~ "E5CA",
                                         all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "E5CA",
                                         all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "E5CAEMS",
                                         all_data$E5 == 1 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "E5EMS",
                                         all_data$E5 == 1 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "E5CA",
                                         
                                         all_data$E5 == 0 & all_data$CA == 0 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "EMSO365",
                                         all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 0 & all_data$O365 == 1 ~ "CAO365",
                                         all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 0 ~ "CAEMS",
                                         all_data$E5 == 0 & all_data$CA == 1 & all_data$EMSE5 == 1 & all_data$O365 == 1 ~ "CAEMS")


summary <- all_data %>% group_by(RecommendationType) %>% summarise(count= n()) ## Output for SMC insights meeting ##

#### Getting the final data together ####

a <- full_join(o365, ems, by = "FinalTPID") %>% full_join(., ca, by = "FinalTPID") %>% full_join(., e5, by = "FinalTPID")
b <- inner_join(a, all_data, by = "FinalTPID")

smc <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\SMCTPIDs.csv")
colnames(smc)[1] <- c("FinalTPID")
smc <- smc %>% select(FinalTPID)

msxi <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\Adhoc\\MSXi.xlsx", sheet = "Sheet2")
msxi <- msxi %>% select(TPID, OverallPositionID_CM01)
colnames(msxi) <- c("FinalTPID", "Box")

b <- inner_join(msxi, b, by = "FinalTPID")
b <- inner_join(smc, b, by = "FinalTPID")

b$Recommendation <- case_when(b$RecommendationType == "E5" ~ b$Recommendation.y.y, 
                              b$RecommendationType == "CA" ~ b$Recommendation.x.x,  
                              b$RecommendationType == "EMS" ~ b$Recommendation.y, 
                              b$RecommendationType == "O365" ~ b$Recommendation.x, 
                              b$RecommendationType == "E5O365" & (b$Box == 8) ~ b$Recommendation.x, 
                              b$RecommendationType == "E5O365" & (b$Box != 8) ~ b$Recommendation.y.y, 
                              b$RecommendationType == "E5EMS" ~ b$Recommendation.y.y, 
                              b$RecommendationType == "E5CA" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) ~ b$Recommendation.x.x,
                              b$RecommendationType == "E5CA" & (b$Box != 5 | b$Box != 6 | b$Box != 7 | b$Box != 8) ~ b$Recommendation.y.y,
                              b$RecommendationType == "E5CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8)  ~ b$Recommendation.x.x, 
                              b$RecommendationType == "E5CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8)  ~ b$Recommendation.y.y,
                              b$RecommendationType == "CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ b$Recommendation.x.x, 
                              b$RecommendationType == "CAEMS" & (b$Box != 5 |b$Box != 6 | b$Box != 7 | b$Box != 8) ~ b$Recommendation.y, 
                              b$RecommendationType == "CAO365" ~ b$Recommendation.x.x, 
                              b$RecommendationType == "EMSO365" & (b$Box == 7 | b$Box == 8) ~ b$Recommendation.x, 
                              b$RecommendationType == "EMSO365" & (b$Box != 7 | b$Box != 8) ~ b$Recommendation.y
                              )   

b$Propensity <- case_when(b$RecommendationType == "E5" ~ b$Propensity.y.y, 
                              b$RecommendationType == "CA" ~ b$Propensity.x.x,  
                              b$RecommendationType == "EMS" ~ b$Propensity.y, 
                              b$RecommendationType == "O365" ~ b$Propensity.x, 
                              b$RecommendationType == "E5O365" & (b$Box == 8) ~ b$Propensity.x,
                              b$RecommendationType == "E5O365" & (b$Box != 8) ~ b$Propensity.y.y,
                              b$RecommendationType == "E5EMS" ~ b$Propensity.y.y, 
                              b$RecommendationType == "E5CA" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) ~ b$Propensity.x.x, 
                              b$RecommendationType == "E5CA" & (b$Box != 5 | b$Box != 6 | b$Box != 7 | b$Box != 8) ~ b$Propensity.y.y,
                              b$RecommendationType == "E5CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ b$Propensity.x.x, 
                              b$RecommendationType == "E5CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) ~ b$Propensity.y.y, 
                              b$RecommendationType == "CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ b$Propensity.x.x, 
                              b$RecommendationType == "CAEMS" & (b$Box != 5 |b$Box != 6 | b$Box != 7 | b$Box != 8) ~ b$Propensity.y, 
                              b$RecommendationType == "CAO365" ~ b$Propensity.x.x, 
                              b$RecommendationType == "EMSO365" & (b$Box == 7 | b$Box == 8) ~ b$Propensity.x, 
                              b$RecommendationType == "EMSO365" & (b$Box != 7 | b$Box != 8) ~ b$Propensity.y)

b$SalesPlay <- case_when(b$RecommendationType == "E5" ~ "Security", 
                          b$RecommendationType == "CA" ~ "Secure Remote Work",  
                          b$RecommendationType == "EMS" ~ "Security", 
                          b$RecommendationType == "O365" ~ "Secure Remote Work", 
                          b$RecommendationType == "E5O365" & (b$Box == 8) ~ "Secure Remote Work", 
                          b$RecommendationType == "E5O365" & (b$Box != 8) ~ "Security",
                          b$RecommendationType == "E5EMS" ~ "Security", 
                          b$RecommendationType == "E5CA" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) ~ "Secure Remote Work",
                          b$RecommendationType == "E5CA" & (b$Box != 5 | b$Box != 6 | b$Box != 7 | b$Box != 8) ~ "Security",
                          b$RecommendationType == "E5CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ "Secure Remote Work", 
                          b$RecommendationType == "E5CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) ~ "Security",
                          b$RecommendationType == "CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ "Secure Remote Work", 
                          b$RecommendationType == "CAEMS" & (b$Box != 5 |b$Box != 6 | b$Box != 7 | b$Box != 8) ~ "Security",
                          b$RecommendationType == "CAO365" ~ "Secure Remote Work", 
                          b$RecommendationType == "EMSO365" & (b$Box == 7 | b$Box == 8) ~ "Secure Remote Work",
                          b$RecommendationType == "EMSO365" & (b$Box != 7 | b$Box != 8) ~ "Security")


b$Product <- case_when(b$RecommendationType == "E5" ~ "M365 E5", 
                       b$RecommendationType == "CA" ~ "M365 E3",  
                       b$RecommendationType == "EMS" ~ "EMS E5", 
                       b$RecommendationType == "O365" ~ "O365", 
                       b$RecommendationType == "E5O365" & (b$Box == 8) ~ "O365", 
                       b$RecommendationType == "E5O365" & (b$Box != 8) ~ "M365 E5",
                       b$RecommendationType == "E5EMS" ~ "M365 E5",  
                       b$RecommendationType == "E5CA" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) ~ "M365 E3",
                       b$RecommendationType == "E5CA" & (b$Box != 5 | b$Box != 6 | b$Box != 7 | b$Box != 8) ~ "M365 E5", 
                       b$RecommendationType == "E5CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ "M365 E3", 
                       b$RecommendationType == "E5CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) ~ "M365 E5", 
                       b$RecommendationType == "CAEMS" & (b$Box == 5 |b$Box == 6 | b$Box == 7 | b$Box == 8) ~ "M365 E3", 
                       b$RecommendationType == "CAEMS" & (b$Box != 5 |b$Box != 6 | b$Box != 7 | b$Box != 8) ~ "EMS E5",
                       b$RecommendationType == "CAO365" ~ "M365 E3", 
                       b$RecommendationType == "EMSO365" & (b$Box == 7 | b$Box == 8) ~ "O365",
                       b$RecommendationType == "EMSO365" & (b$Box != 7 | b$Box != 8) ~ "EMS E5")

b$WhyRecommended <- case_when(b$RecommendationType == "E5" ~ b$WhyRecommended.y.y, 
                              b$RecommendationType == "CA" ~ b$WhyRecommended.x.x,  
                              b$RecommendationType == "EMS" ~ b$WhyRecommended.y, 
                              b$RecommendationType == "O365" ~ b$WhyRecommended.x, 
               
                              b$RecommendationType == "E5O365" & (b$Box == 8) & grepl("high", b$Recommendation.y.y) == TRUE ~ paste(b$WhyRecommended.x, "\\r\\n", "This customer also has high propensity for M365 E5."), 
                              b$RecommendationType == "E5O365" & (b$Box == 8) & grepl("medium", b$Recommendation.y.y) == TRUE ~ paste(b$WhyRecommended.x, "\\r\\n", "This customer also has medium propensity for M365 E5."), 
                              b$RecommendationType == "E5O365" & (b$Box != 8) & grepl("high", b$Recommendation.x) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has high propensity for O365."), 
                              b$RecommendationType == "E5O365" & (b$Box != 8) & grepl("medium", b$Recommendation.x) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has medium propensity for O365."), 
                              
                              b$RecommendationType == "E5EMS" & grepl("high", b$Recommendation.y) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has high propensity for EMS E5 seats."), 
                              b$RecommendationType == "E5EMS" & grepl("medium", b$Recommendation.y) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has medium propensity for EMS E5."), 

                              b$RecommendationType == "E5CA" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) & grepl("high", b$Recommendation.y.y) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has high propensity for M365 E5."), 
                              b$RecommendationType == "E5CA" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) & grepl("medium", b$Recommendation.y.y) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has medium propensity for M365 E5."), 
                              b$RecommendationType == "E5CA" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) & grepl("high", b$Recommendation.x.x) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has high propensity for M365 E3."), 
                              b$RecommendationType == "E5CA" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) & grepl("medium", b$Recommendation.x.x) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has medium propensity for M365 E3."), 
                              
                            
                              b$RecommendationType == "E5CAEMS" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) & grepl("high", b$Recommendation.y.y) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has high propensity for M365 E5."), 
                              b$RecommendationType == "E5CAEMS" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) & grepl("medium", b$Recommendation.y.y) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has medium propensity for M365 E5."),
                              b$RecommendationType == "E5CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) & grepl("high", b$Recommendation.x.x) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has high propensity for M365 E3."), 
                              b$RecommendationType == "E5CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) & grepl("medium", b$Recommendation.x.x) == TRUE ~ paste(b$WhyRecommended.y.y, "\\r\\n", "This customer also has medium propensity for M365 E3."), 
                              
                              b$RecommendationType == "CAEMS" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) & grepl("high", b$Recommendation.y) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has high propensity for EMS E5."), 
                              b$RecommendationType == "CAEMS" & (b$Box == 5 | b$Box == 6 | b$Box == 7 | b$Box == 8) & grepl("medium", b$Recommendation.y) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has medium propensity for EMS E5."),
                              b$RecommendationType == "CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) & grepl("high", b$Recommendation.x.x) == TRUE ~ paste(b$WhyRecommended.y, "\\r\\n", "This customer also has high propensity for M365 E3."), 
                              b$RecommendationType == "CAEMS" & (b$Box != 5 |b$Box != 7 | b$Box != 7 | b$Box != 8) & grepl("medium", b$Recommendation.x.x) == TRUE ~ paste(b$WhyRecommended.y, "\\r\\n", "This customer also has medium propensity for M365 E3."), 
                              
                              b$RecommendationType == "CAO365" & grepl("high", b$Recommendation.x) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has high propensity for O365."), 
                              b$RecommendationType == "CAO365" & grepl("medium", b$Recommendation.x) == TRUE ~ paste(b$WhyRecommended.x.x, "\\r\\n", "This customer also has medium propensity for O365."), 
                              
                              b$RecommendationType == "EMSO365" & (b$Box == 7 | b$Box == 8) & grepl("high", b$Recommendation.y) == TRUE ~ paste(b$WhyRecommended.x, "\\r\\n", "This customer also has high propensity for EMS E5."), 
                              b$RecommendationType == "EMSO365" & (b$Box == 7 | b$Box == 8) & grepl("medium", b$Recommendation.y) == TRUE ~ paste(b$WhyRecommended.x, "\\r\\n", "This customer also has medium propensity for EMS E5."),
                              b$RecommendationType == "EMSO365" & (b$Box != 7 | b$Box != 8) & grepl("high", b$Recommendation.x) == TRUE ~ paste(b$WhyRecommended.y, "\\r\\n", "This customer also has high propensity for O365."), 
                              b$RecommendationType == "EMSO365" & (b$Box != 7 | b$Box != 8) & grepl("medium", b$Recommendation.x) == TRUE ~ paste(b$WhyRecommended.y, "\\r\\n", "This customer also has medium propensity for O365.")) 

b$ModelName <- "IDEAs MW Upsell Models"
b$CreatedDate <- "2020/09/01"

b <- b %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity, SalesPlay, Product, ModelName, CreatedDate, RecommendationType)

#### Potential Revenue Calculation ####
ems_seats <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\EMSE5Seats_June.csv")
m365_seats <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\ActualsOverlay\\ResCubeO365Seats.xlsx")
m365_seats$Date <- as.character(m365_seats$Date)
m365_seats <- m365_seats %>% filter(Date == "2020-08-31") %>% select(FinalTPID, M365E3Seats, M365E5Seats, TotalM365Seats)
b <- left_join(b, ems_seats, by = "FinalTPID")
b <- left_join(b, m365_seats, by = "FinalTPID")
b[is.na(b)] <- 0

b$Revenue <- case_when(b$Recommendation %in% c("This customer has a high probability to purchase 300+ M365 E5 seats in the next 6 months", 
                                               "This customer has a medium probability to purchase 300+ M365 E5 seats in the next 6 months") & b$M365E5Seats < 300 ~ (300 - b$M365E5Seats) * 12 * 57 * b$Propensity/100, 
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ M365 E5 seats in the next 6 months", 
                                               "This customer has a medium probability to purchase 300+ M365 E5 seats in the next 6 months") & b$M365E5Seats >= 300 ~ (300) * 12 * 57 * b$Propensity/100, 
                       
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ M365 seats in the next 6 months", 
                                               "This customer has a medium probability to purchase 300+ M365 seats in the next 6 months") & b$TotalM365Seats <  300 ~ (300 - b$TotalM365Seats) * 12 * 32 * b$Propensity/100,
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ M365 seats in the next 6 months", 
                                               "This customer has a medium probability to purchase 300+ M365 seats in the next 6 months") & b$TotalM365Seats >=  300 ~ 300 * 12 * 32 * b$Propensity/100,
                       
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ EMS E5 seats in the next year", 
                                               "This customer has a medium probability to purchase 300+ EMS E5 seats in the next year") & b$EMSE5Seats <  300 ~ (300 - b$EMSE5Seats) * 12 * 14.80 * b$Propensity/100,
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ EMS E5 seats in the next year", 
                                               "This customer has a medium probability to purchase 300+ EMS E5 seats in the next year") & b$EMSE5Seats >=  300 ~ 300 * 12 * 14.80 * b$Propensity/100,
                       
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ O365 seats in the next year", 
                                               "This customer has a medium probability to purchase 300+ O365 seats in the next year") ~ 300 * 12 * 20 * b$Propensity/100)


b$WinningModel <- case_when(b$Recommendation %in% c("This customer has a high probability to purchase 300+ M365 E5 seats in the next 6 months", 
                                               "This customer has a medium probability to purchase 300+ M365 E5 seats in the next 6 months") ~ "M365E5", 
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ M365 seats in the next 6 months", 
                                               "This customer has a medium probability to purchase 300+ M365 seats in the next 6 months") ~ "M365E3",
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ EMS E5 seats in the next year", 
                                               "This customer has a medium probability to purchase 300+ EMS E5 seats in the next year") ~ "EMSE5",
                       b$Recommendation %in% c("This customer has a high probability to purchase 300+ O365 seats in the next year", 
                                               "This customer has a medium probability to purchase 300+ O365 seats in the next year") ~ "O365")



#EXPERIMENTATION TPIDS
final_treatment <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalTreatmentTPIDs.csv")
final_treatment <- final_treatment %>% select(FinalTPID, ExpScenario)

final_treatment <- inner_join(final_treatment, b, by = "FinalTPID")
write.csv(final_treatment, "C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutputTreatment.csv", row.names = FALSE)

final_control <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\FinalControlTPIDs.csv")
final_control <- final_control %>% select(FinalTPID, ExpScenario)

final_control <- inner_join(final_control, b, by = "FinalTPID")
write.csv(final_control, "C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutputControl.csv", row.names = FALSE)

#Easier way to write out to DR SQL - 
final_treatment1 <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutputTreatment.csv")
final_treatment1 <- final_treatment1 %>% select(FinalTPID, Recommendation, WhyRecommended, Propensity, Product, SalesPlay, ModelName, CreatedDate)
final_treatment1$Recommendation <- paste(final_treatment1$Recommendation, '.', sep = "")
final_treatment1$Propensity <- final_treatment1$Propensity/100

final_treatment1$FinalTPID <- as.character(final_treatment1$FinalTPID)
final_treatment1$Propensity <- as.character(final_treatment1$Propensity)
write.table(final_treatment1, "C:\\Users\\varamase\\Documents\\DataStreams\\SalesModelsFY21\\FinalData\\DROutputTreatmentForSQL.csv", row.names = FALSE, sep = ";")


# Analysis for future verification of logic - 
# OverallPositionID_CM01 count
# <dbl> <int>
#   1                      1   126
# 2                      2   142
# 3                      3   143
# 4                      4   423
# 5                      5     5
# 6                      6   103
# Since all above are mostly in everything beyond 2B (E5 + CA output), decided to pick E5 as the winner, as CA is expansion only



                                            
                                            
                                            
                                      

