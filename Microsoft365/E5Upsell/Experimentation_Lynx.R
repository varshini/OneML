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
data$Propensity <- case_when(data$Probability >= 60 ~ "High propensity for M365 E5", 
                          data$Probability >= 46 & data$Probability < 60 ~ "Medium propensity for M365 E5")
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
  c <- strsplit(y, " ")[[1]]
  paste(toupper(substring(c, 1,1)), substring(c, 2),
        sep="", collapse=" ")
}

model_output_reasons$WhyRecommended <- gsub("^Presence of", "", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("Presence of", "presence of", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("High", "high", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("The", "the", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub("higher conversion rate", "higher conversion rate to M365 E5", model_output_reasons$WhyRecommended)
model_output_reasons$WhyRecommended <- gsub(", and", "and", model_output_reasons$WhyRecommended)
#Capitalize the first letter
model_output_reasons$WhyRecommended <- sapply(model_output_reasons$WhyRecommended , CapStr)

#Sales Motion
model_output_reasons$SalesMotion <- 


write.csv(data, "C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\ProcessedModelOutput.csv", row.names = FALSE)

# e5 upsell and customer adds output combined 

e5_output <- inner_join(m365e3, model_output, by = "TPID")
ca_output <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365E5Upsell\\M365EnterpriseCAOutput.csv")
colnames(ca_output)[1] <- "TPID"
ca_output_hm <- ca_output %>% filter(Propensity == "H" | Propensity == "M")

ca_output_hm1 <- inner_join(m365e3, ca_output_hm, by = "TPID")


