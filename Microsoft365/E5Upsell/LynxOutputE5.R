#Processing for E5 upsell model - enterprise customers#
library(dplyr)
library(readxl)

model_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\ModelOutput.csv")
colnames(model_output)[1] <- "TPID"
ent_target_list <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\EnterpriseCustomerProfile.xlsx")
ent_target_list <- ent_target_list %>% filter(`Enterprise Profile` != "(5) M365 E5")
# m365e3 <- ent_target_list %>% filter(`Enterprise Profile` == "(4) M365 E3")
# m365e3$M365E3 <- 1
  
data <- inner_join(ent_target_list, model_output, by = "TPID")
data$Propensity <- case_when(data$Probability >= 60 ~ "Account shows high propensity to upsell to M365 E5", 
                          data$Probability >= 45 & data$Probability < 60 ~ "Account shows medium propensity to upsell to M365 E5")
data <- data %>% filter(Propensity != "NA")
#data <- left_join(data, m365e3, by = "TPID")
#data[is.na(data)] <- 0
write.csv(data, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\ProcessedModelOutput.csv", row.names = FALSE)

# Model reasons 
model_output_reasons <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\ModelOutputReasons.csv")
colnames(model_output_reasons)[1] <- "TPID"
model_output_reasons <- inner_join(data, model_output_reasons, by = "TPID")
model_output_reasons <- model_output_reasons %>% dplyr::select(TPID, Propensity, Probability, `Why.Recommended.with.usage`)
colnames(model_output_reasons)[4] <- "WhyRecommended"

CapStr <- function(y) {
  paste(toupper(substring(y,1,1)), substring(y,2), sep = "")
}

model_output_reasons$WhyRecommended <- gsub("^Presence of", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("^ ", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- paste("Presence of", model_output_reasons$WhyRecommended, sep = " ")
model_output_reasons$WhyRecommended <- gsub("Presence of", "presence of", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("High", "high", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("The Account", "the account", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("higher conversion rate", "higher conversion rate to M365 E5", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("higher rate of conversion", "higher rate of conversion to M365 E5", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("is linked with", "are linked to", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub(", and", " and", model_output_reasons$WhyRecommended)

# many higher rate of conversion text 

# there is a full stop at the end of the sentence, remove that and then add the sentences 
model_output_reasons$WhyRecommended <- gsub("\\.", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("linked to higher conversion rate to M365 E5", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("linked to ME5 adoption", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("which are linked to higher rate of conversion to M365 E5$", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- paste(model_output_reasons$WhyRecommended, "linked to higher conversion rate to M365 E5", sep = "")

# model_output_reasons$WhyRecommended <- ifelse(endsWith(model_output_reasons$WhyRecommended, "M365 E5") == FALSE, 
#                                               paste(model_output_reasons$WhyRecommended, "linked to higher conversion rate to M365 E5", sep = ""), 
#                                                     model_output_reasons$WhyRecommended)
#model_output_reasons$WhyRecommended <- paste(model_output_reasons$WhyRecommended, "linked to higher conversion rate to M365 E5", sep = "")

#Capitalize the first letter
model_output_reasons$WhyRecommended <- sapply(model_output_reasons$WhyRecommended , CapStr)

#Sales Motion
model_output_reasons$SalesMotion <- "M365 E5"
model_output_reasons$ActionByDate <- "2020/03/31"
model_output_reasons$RunDate <- "2019/06/30"

write.csv(model_output_reasons, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\FinalModelOutput.csv", row.names = FALSE)

###############################################


# e5 upsell, customer adds and seat expansion combined 

ca_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\M365EnterpriseCAOutput.csv")
colnames(ca_output)[1] <- "TPID"
ca_output_hm <- ca_output %>% filter(Propensity == "H" | Propensity == "M")

ca_e5 <- inner_join(data, ca_output_hm, by = "TPID")

seat_exp <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\2019-12-07_Seat_Expansion_Output.csv")
seat_exp$delta_flag <- ifelse(seat_exp$Propensity >= 500, 'expansion', 'no expansion')
seat_exp1 <- seat_exp %>% filter(delta_flag == "expansion")

e5_seatexp <- data.frame(setdiff(seat_exp1$TPID, data$TPID))
colnames(e5_seatexp)[1] <- "TPID"
e5_seatexp_ca <- data.frame(setdiff(e5_seatexp$TPID, ca_output_hm$TPID))

