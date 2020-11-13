library(readxl)
library(dplyr)
library(tidyverse)
library(data.table)

tranches <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\VCTranches.xlsx")
tranches <- tranches %>% select(TPID, `Box #`)
colnames(tranches) <- c("FinalTPID", "Tranche")

model_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\LIMERecommendationsv2.csv")
model_output <- model_output %>% select(FinalTPID, WhyRecommended, Propensity)

scoring_data <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Data\\Features\\ScoringFeaturesFinalv2.csv")
scoring_data <- scoring_data %>% select(FinalTPID, M365F1, M365E3, M365E5, EXOPaidAvailableUnits)
scoring_data$TotalM365 <- scoring_data$M365F1 + scoring_data$M365E3 + scoring_data$M365E5

a <- inner_join(model_output, tranches) 
a <- a %>% group_by(Tranche) %>% summarise(count = n()) # Overall account distribution in tranches

model_output <- filter(model_output, WhyRecommended != "NA")

output <- inner_join(model_output, tranches)
output <- inner_join(output, scoring_data)
output <- filter(output, !is.na(Tranche))

#Add Recommendation 
# output$Recommendation <- case_when(output$Tranche %in% c("1A", "1B", "1C", "2A") & output$Propensity >= 0.49 ~ "High probability to purchase 300+ M365 seats in the next 6 months", 
#                                    output$Tranche %in% c("1A", "1B", "1C", "2A") & output$Propensity >= 0.34 & output$Propensity < 0.49 ~ "Medium probability to purchase 300+ M365 seats in the next 6 months", 
#                                    output$Tranche %in% c("2B", "2C", "3B", "3C") & output$Propensity >= 0.61 & output$Propensity < 0.70 & (output$TotalM365 < 0.90 * output$EXOPaidAvailableUnits) ~ "Medium probability to expand 300+ M365 seats in the next 6 months", 
#                                    output$Tranche %in% c("2B", "2C", "3B", "3C") & output$Propensity >= 0.70 & (output$TotalM365 < 0.90 * output$EXOPaidAvailableUnits) ~ "High probability to expand 300+ M365 seats in the next 6 months")

output$Recommendation <- case_when(output$Tranche %in% c("1A", "1B", "1C", "2A") & output$Propensity >= 0.49 ~ "High probability to purchase 300+ M365 seats in the next 6 months", 
                                   output$Tranche %in% c("1A", "1B", "1C", "2A") & output$Propensity >= 0.34 & output$Propensity < 0.49 ~ "Medium probability to purchase 300+ M365 seats in the next 6 months", 
                                   output$Tranche %in% c("2B", "2C", "3B", "3C") & output$Propensity >= 0.49 & (output$TotalM365 < 0.90 * output$EXOPaidAvailableUnits) ~ "High probability to expand 300+ M365 seats in the next 6 months")
                                   
output <- filter(output, !is.na(Recommendation))

b <- output %>% group_by(Recommendation) %>% summarise(count = n()) # Overall account distribution in tranches

# Add Sales Play and Product Mapping
output$SalesPlay <- case_when(output$Tranche %in% c("1A", "1B", "1C", "2A") ~ "Secure Remote Work", 
                              output$Tranche %in% c("2B", "2C", "3B", "3C") ~ "Security")

output$Product <- case_when(output$Tranche %in% c("1A", "1B", "1C", "2A") ~ "M365 E3", 
                              output$Tranche %in% c("2B", "2C", "3B", "3C") ~ "M365 E5")

write.csv(output, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\M365NCAFY21\\Modeling\\DROutput\\FinalDROutput.csv", row.names = FALSE)




