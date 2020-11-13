library(dplyr)
options(stringsAsFactors = FALSE)

# all seat expansion
seat_exp <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\LynxOutputs\\SeatExpansionOnly.csv")
colnames(seat_exp) <- c("TPID", "Propensity", "WhyRecommended", "Probability", "ActionByDate")
seat_exp$SalesMotion <- "M365 Premium Seat Expansion"
seat_exp <- seat_exp %>% select(TPID, Propensity, Probability, WhyRecommended, SalesMotion, ActionByDate)
# Normalizing seat expansion seat count between 0 and 1 so that it is comparable to the other models for sorting
seat_exp$Probability <- (seat_exp$Probability - min(seat_exp$Probability))/(max(seat_exp$Probability) - min(seat_exp$Probability))
seat_exp$Probability <- round(seat_exp$Probability * 100, digits = 0)
seat_exp$ActionByDate <- "3/31/2020"

# e5 - only + seat exp
e5_output <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\LynxOutputs\\E5LynxOutput.tsv", sep = "\t")
e5_output <- e5_output %>% select(TPID, Propensity, Probability, WhyRecommended, SalesMotion, ActionByDate)
e5_output$ActionByDate <- "3/31/2020"
e5_output<- left_join(e5_output, seat_exp, by = "TPID")
e5_output$FinalPropensity <- case_when(grepl("ME5", e5_output$Propensity.y) == TRUE ~  
                                    paste(e5_output$Propensity.x, "and", e5_output$Propensity.y, sep = " "), 
                                    TRUE ~ e5_output$Propensity.x)

# while merging, all other features come from the winning model only
e5_output <- e5_output %>% dplyr::select(TPID, FinalPropensity, Probability.x, WhyRecommended.x, SalesMotion.x, ActionByDate.x)
colnames(e5_output) <- c("TPID", "Propensity", "Probability", "WhyRecommended", "SalesMotion", "ActionByDate")

# nca - only + seat exp
nca_output <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\LynxOutputs\\ENCA_FinalModelOutput.csv")
colnames(nca_output)[1] <- "TPID"
only_nca <- data.frame(setdiff(nca_output$TPID, e5_output$TPID))
colnames(only_nca)<- "TPID"
only_nca <- inner_join(only_nca, nca_output, by = "TPID")
only_nca$SalesMotion <- "M365 Customer Adds"
only_nca<- left_join(only_nca, seat_exp, by = "TPID")
only_nca$FinalPropensity <- case_when(grepl("ME3", only_nca$Propensity.y) == TRUE ~  
                                         paste(only_nca$Propensity.x, "and", only_nca$Propensity.y, sep = " "), 
                                       TRUE ~ only_nca$Propensity.x)
only_nca <- only_nca %>% dplyr::select(TPID, FinalPropensity, Probability.x, WhyRecommended.x, SalesMotion.x, ActionByDate.x)
colnames(only_nca) <- c("TPID", "Propensity", "Probability", "WhyRecommended", "SalesMotion", "ActionByDate")

final_output <- rbind(e5_output, only_nca)

# Only seat exp
only_seatexp <- data.frame(setdiff(seat_exp$TPID, final_output$TPID))
colnames(only_seatexp)<- "TPID"
only_seatexp <- inner_join(only_seatexp, seat_exp, by = "TPID")


final_output <- rbind(final_output, only_seatexp)

# clean up text 
final_output$Propensity <- gsub("and The account","and",final_output$Propensity)
final_output$Propensity <- gsub("ME5","M365 E5",final_output$Propensity)
final_output$Propensity <- gsub("ME3","M365 E3",final_output$Propensity)

write.csv(final_output, "C:\\Users\\varamase\\OneDrive - Microsoft\\LynxOutputs\\CombinedUpsellModelOutputv2.csv", row.names = FALSE)



# Converting initial lynx output to PBI report format - adding account name and other columns - TODO
lynx_output <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\LynxOutputs\\CombinedUpsellModelOutput.csv")

pbi_output <- lynx_output %>% dplyr::select(TPID, Propensity, WhyRecommended, SalesMotion)
pbi_output$ModelScenario <- case_when(pbi_output$SalesMotion == "M365 E5" ~ "M365E5",
                                      pbi_output$SalesMotion == "M365 Customer Adds" ~ "M365CA",
                                      pbi_output$SalesMotion == "M365 Premium Seat Expansion" ~ "SeatExpansion",
                                      TRUE ~ "NA")
pbi_output$MonthEndDate <- "1/31/2020"

# Adding other attributes required for PBI 

#Area, Account Name, Propensity Shortened, Seat Expansion thresholds
pbi_output$PropensityShortened <- case_when(grepl("high", pbi_output$Propensity) == TRUE ~ "High", 
                                            grepl("medium", pbi_output$Propensity) == TRUE ~ "Medium", 
                                            TRUE ~ "")

pbi_output$SeatExpansionThresholds <-  case_when(grepl("> 2k seats for ME5", pbi_output$Propensity) == TRUE ~ ">2k ME5",
                                                 grepl("> 1.2k seats for ME5", pbi_output$Propensity) == TRUE ~ ">1.2k ME5",
                                                 grepl("> 300 seats for ME5", pbi_output$Propensity) == TRUE ~ ">300 ME5",
                                                 grepl("> 500 seats for ME5", pbi_output$Propensity) == TRUE ~ ">500 ME5",
                                                 grepl("> 100 seats for ME5", pbi_output$Propensity) == TRUE ~ ">100 ME5",
                                                 grepl("> 2k seats for ME3", pbi_output$Propensity) == TRUE ~ ">2k ME3",
                                                 grepl("> 1.2k seats for ME3", pbi_output$Propensity) == TRUE ~ ">1.2k ME3",
                                                 grepl("> 300 seats for ME3", pbi_output$Propensity) == TRUE ~ ">300 ME3",
                                                 grepl("> 500 seats for ME3", pbi_output$Propensity) == TRUE ~ ">500 ME3",  
                                                 grepl("> 100 seats for ME3", pbi_output$Propensity) == TRUE ~ ">100 ME3",
                                                 grepl("expand", pbi_output$Propensity) == TRUE ~ "Other",
                                                 TRUE ~ "")
#Area, Account Name
mal_data <- read.csv("C:\\Users\\varamase\\OneDrive - Microsoft\\MALData.csv")
colnames(mal_data) <- c("TPID", "AccountName", "AreaName")
pbi_output <- inner_join(pbi_output, mal_data, by = "TPID")
pbi_output$AreaName <- case_when(pbi_output$AreaName == "Western Europe" ~ "WE",
                                 pbi_output$AreaName == "Central and Eastern Europe" ~ "CEE", 
                                 TRUE ~ pbi_output$AreaName)

pbi_output$AccountName <- gsub("'", "", pbi_output$AccountName)
pbi_output <- pbi_output %>% dplyr::select(TPID, Propensity, WhyRecommended, ModelScenario, AreaName, MonthEndDate, PropensityShortened, SeatExpansionThresholds, AccountName)

write.csv(pbi_output, "C:\\Users\\varamase\\OneDrive - Microsoft\\PBIReportOutput.csv", row.names = FALSE)
