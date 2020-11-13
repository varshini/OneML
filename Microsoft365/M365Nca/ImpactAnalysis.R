######### M365 impact analysis ###########

# TODO - split positive dispositon rate, # of opportunities, pipeline impact between the 3 models (think after talking to Gorkem)
# TODO - split between high propensity and medium propensity?

drdata1 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\ImpactAnalysis\\DRData.xlsx", sheet = "Current")
drdata2 <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\ImpactAnalysis\\DRData.xlsx", sheet = "Actioned")
drdata2action <- filter(drdata2, FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL' | FeedbackType == '3' )


drdata1tpid <- data.frame(unique(drdata1$TPID))
colnames(drdata1tpid) <- "TPID"
drdata2tpid <- data.frame(unique(drdata2$TPID))
colnames(drdata2tpid) <- "TPID"
drdatatpid <- rbind(drdata1tpid, drdata2tpid) 

mytreatment <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\FinalDROutput\\Experimentation\\M365UpsellModelFinalOutputTreatment.csv")
mytreatmenttpid <- data.frame(unique(mytreatment$TPID))
colnames(mytreatmenttpid) <- "TPID"

a <- setdiff(mytreatmenttpid$TPID, drdatatpid$TPID)  # missing TPIDS from DR
write.csv(a, "C:\\Users\\varamase\\Documents\\DataStreams\\M365NCA\\ImpactAnalysis\\MissingTPIDS.csv", row.names = FALSE)

# M365 NCA
m365nca <- filter(mytreatment, Product == "M365 E3" & WhyRecommended != "Account has high O365 online presence and low penetration of On-Prem licenses linked to higher conversion rate of M365 products")
m365ncatpid <- data.frame(unique(m365nca$TPID))
colnames(m365ncatpid) <- "TPID"
m365nca1 <- inner_join(m365ncatpid, drdata1, by= "TPID")  #unique recommendations check
m365nca2 <- inner_join(m365ncatpid, drdata2, by= "TPID") 
m365nca3 <- filter(m365nca2, ((FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL') | (FeedbackType == '3' & FeedbackReasonCode == '1'))) 
m365nca2 <- filter(m365nca2, OpportunityEstimatedValue != "NULL")
m365nca2$OpportunityEstimatedValue <- as.numeric(m365nca2$OpportunityEstimatedValue)
sum(m365nca2$OpportunityEstimatedValue)

# M365 NCA - differentiate between high and medium
nca_high <- filter(m365nca, RecommendedAction == "This customer has a high probability to purchase 300+ M365 seats in the next 6 months")
nca_high <- data.frame(unique(nca_high$TPID))
colnames(nca_high)[1] <- "TPID"
m365nca2 <- inner_join(nca_high, drdata2, by= "TPID") 

nca_medium <- filter(m365nca, RecommendedAction == "This customer has a medium probability to purchase 300+ M365 seats in the next 6 months")
nca_medium <- data.frame(unique(nca_medium$TPID))
colnames(nca_medium)[1] <- "TPID"
m365nca2 <- inner_join(nca_medium, drdata2, by= "TPID") 

# E5
e5 <- filter(mytreatment, Product == "M365 E5")
e5tpid <- data.frame(unique(e5$TPID))
colnames(e5tpid) <- "TPID"
e5tpid1 <- inner_join(e5, drdata1, by= "TPID") 
e5tpid2 <- inner_join(e5, drdata2, by= "TPID") 
e5tpid3 <- filter(e5tpid2, ((FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL') | (FeedbackType == '3' & FeedbackReasonCode == '1'))) 
e5tpid2 <- filter(e5tpid2, OpportunityEstimatedValue != "NULL")
e5tpid2$OpportunityEstimatedValue <- as.numeric(e5tpid2$OpportunityEstimatedValue)
sum(e5tpid2$OpportunityEstimatedValue)

#Alperen
alp <- filter(mytreatment, Product == "M365 E3" & WhyRecommended == "Account has high O365 online presence and low penetration of On-Prem licenses linked to higher conversion rate of M365 products")
alptpid <- data.frame(unique(alp$TPID))
colnames(alptpid) <- "TPID"
alptpid1 <- inner_join(alp, drdata1, by= "TPID") 
alptpid2 <- inner_join(alp, drdata2, by= "TPID") 
alptpid3 <- filter(alptpid2, ((FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL') | (FeedbackType == '3' & FeedbackReasonCode == '1'))) 
alptpid2 <- filter(alptpid2, OpportunityEstimatedValue != "NULL")
alptpid2$OpportunityEstimatedValue <- as.numeric(alptpid2$OpportunityEstimatedValue)
sum(alptpid2$OpportunityEstimatedValue)

######### Teams impact analysis ###########

treatment <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROI\\TeamsForecasting\\Model\\Corrected\\TeamsAERecommendationTextSMCManagedTreatment.csv") #8417
control <- read.csv("C:\\Users\\varamase\\Documents\\DataStreams\\ROI\\TeamsForecasting\\Model\\Corrected\\TeamsAERecommendationTextSMCManagedControl.csv")
monthly_aeqe <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROI\\TeamsForecasting\\ImpactAnalysis\\TeamsAEQE.xlsx")
dr_output_current <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROI\\TeamsForecasting\\ImpactAnalysis\\TeamsDROutput.xlsx", sheet = "Current")
dr_output_actioned <- read_excel("C:\\Users\\varamase\\Documents\\DataStreams\\ROI\\TeamsForecasting\\ImpactAnalysis\\TeamsDROutput.xlsx", sheet = "Actioned")

dr_output_current <- filter(dr_output_current, recommendedaction %in% c("This customer's Teams potential is not expected to grow in the next year. Contact the customer and unblock Teams usage", 
                             "This customer is expected to grow Teams adoption by 50%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" , 
                             "This customer is expected to grow Teams adoption by 20%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" ,        
                             "This customer is expected to grow Teams adoption by 10%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption"  ,       
                             "This customer is expected to have Teams usage decline by 5%+ within the next year. Contact the customer to remove any blockers and prevent decline in usage"))

# Reviewed, Positive disposition rate
dr_output_action <- filter(dr_output_actioned, Recommendation %in% c("This customer's Teams potential is not expected to grow in the next year. Contact the customer and unblock Teams usage", 
                                                                         "This customer is expected to grow Teams adoption by 50%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" , 
                                                                         "This customer is expected to grow Teams adoption by 20%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" ,        
                                                                         "This customer is expected to grow Teams adoption by 10%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption"  ,       
                                                                         "This customer is expected to have Teams usage decline by 5%+ within the next year. Contact the customer to remove any blockers and prevent decline in usage"))
dr_output_action <- filter(dr_output_action, FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL' | FeedbackType == '3' ) #1613

a <- filter(dr_output_action, FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL' | FeedbackType == '3' ) #1613
a <- filter(dr_output_action, ((FeedbackType == '1' | FeedbackType == '4' | FeedbackType == 'NULL') | (FeedbackType == '3' & FeedbackReasonCode == '1'))) #1193

#Impact 

actiontpid <- data.frame(unique(dr_output_action$TPID))
colnames(actiontpid)[1] <- "TPID"
actiontpid <- inner_join(actiontpid, treatment, by = "TPID")

currenttpid <-  data.frame(unique(dr_output_current$TPID))
colnames(currenttpid)[1] <- "TPID"
currenttpid <- inner_join(currenttpid, treatment, by = "TPID")

alltpid <- rbind(actiontpid, currenttpid)
alltpid <- data.frame(unique(alltpid$TPID))
colnames(alltpid)[1] <- "TPID"
alltpid <- inner_join(alltpid, treatment, by = "TPID")

# 1) For reviewed accounts - 
#   1a) For growth accounts 

review_growth <- filter(actiontpid, Recommendation %in% c(
                                         "This customer is expected to grow Teams adoption by 50%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" , 
                                         "This customer is expected to grow Teams adoption by 20%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" ,        
                                         "This customer is expected to grow Teams adoption by 10%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption"))

review_growth <- inner_join(review_growth, monthly_aeqe, by = "TPID")
sum(review_growth$Jul2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Jul2019QualifiedEntitlements, na.rm = TRUE)
sum(review_growth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(review_growth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(review_growth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Apr2019QualifiedEntitlements, na.rm = TRUE)


#review_growth$OctUsage <- review_growth$Oct2019ActivatedEntitlements/review_growth$Oct2019QualifiedEntitlements
#review_growth$AprUsage <- review_growth$Apr2019ActivatedEntitlements/review_growth$Apr2019QualifiedEntitlements
#review_growth$OctUsage[is.infinite(review_growth$OctUsage )] <- 0
#review_growth$AprUsage[is.infinite(review_growth$AprUsage )] <- 0
#review_growth$UsageGrowth <- (review_growth$AprUsage - review_growth$OctUsage)/review_growth$OctUsage
#review_growth$UsageGrowth[is.infinite(review_growth$UsageGrowth )] <- 0
# (sum of Oct AE/sum of Oct QE) - 25%, (sum of Apr AE/sum of Apr QE) - 50%

#   1b) For non growth accounts
review_nogrowth <- filter(actiontpid, Recommendation %in% c("This customer's Teams potential is not expected to grow in the next year. Contact the customer and unblock Teams usage"))
review_nogrowth <- inner_join(review_nogrowth, monthly_aeqe, by = "TPID")
# review_nogrowth$OctUsage <- review_nogrowth$Oct2019ActivatedEntitlements/review_nogrowth$Oct2019QualifiedEntitlements
# review_nogrowth$AprUsage <- review_nogrowth$Apr2019ActivatedEntitlements/review_nogrowth$Apr2019QualifiedEntitlements
# review_nogrowth$OctUsage[is.infinite(review_nogrowth$OctUsage )] <- 0
# review_nogrowth$AprUsage[is.infinite(review_nogrowth$AprUsage )] <- 0
# review_nogrowth$UsageGrowth <- (review_nogrowth$AprUsage - review_nogrowth$OctUsage)/review_nogrowth$OctUsage
# review_nogrowth$UsageGrowth[is.infinite(review_nogrowth$UsageGrowth )] <- 0
sum(review_nogrowth$Jul2019ActivatedEntitlements, na.rm = TRUE)/sum(review_nogrowth$Jul2019QualifiedEntitlements, na.rm = TRUE)
sum(review_nogrowth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(review_nogrowth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(review_nogrowth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(review_nogrowth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(review_nogrowth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(review_nogrowth$Apr2019QualifiedEntitlements, na.rm = TRUE)

# 1c) Growth - For >50% accounts 
review_growth <- filter(actiontpid, Recommendation %in% c(
  "This customer is expected to grow Teams adoption by 10%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption"))
review_growth <- inner_join(review_growth, monthly_aeqe, by = "TPID")
nrow(review_growth)
sum(review_growth$Jul2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Jul2019QualifiedEntitlements, na.rm = TRUE)
sum(review_growth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(review_growth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(review_growth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(review_growth$Apr2019QualifiedEntitlements, na.rm = TRUE)



# 2) For non reviewed accounts - 
#   2a) For growth accounts 

no_review_growth <- filter(currenttpid, Recommendation %in% c(
  "This customer is expected to grow Teams adoption by 50%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" , 
  "This customer is expected to grow Teams adoption by 20%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" ,        
  "This customer is expected to grow Teams adoption by 10%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption"))

no_review_growth <- inner_join(no_review_growth, monthly_aeqe, by = "TPID")
sum(no_review_growth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(no_review_growth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(no_review_growth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(no_review_growth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(no_review_growth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(no_review_growth$Apr2019QualifiedEntitlements, na.rm = TRUE)

# 2b) For non growth accounts

no_review_nogrowth <- filter(currenttpid, Recommendation %in% c("This customer's Teams potential is not expected to grow in the next year. Contact the customer and unblock Teams usage"))
no_review_nogrowth <- inner_join(no_review_nogrowth, monthly_aeqe, by = "TPID")
sum(no_review_nogrowth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(no_review_nogrowth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(no_review_nogrowth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(no_review_nogrowth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(no_review_nogrowth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(no_review_nogrowth$Apr2019QualifiedEntitlements, na.rm = TRUE)

# 3) for all accounts
sum(no_review_growth$Oct2019ActivatedEntitlements, na.rm = TRUE) + sum(review_growth$Oct2019ActivatedEntitlements, na.rm = TRUE)
sum(no_review_growth$Oct2019QualifiedEntitlements, na.rm = TRUE) + sum(review_growth$Oct2019QualifiedEntitlements, na.rm = TRUE)

sum(no_review_growth$Apr2019ActivatedEntitlements, na.rm = TRUE) + sum(review_growth$Apr2019ActivatedEntitlements, na.rm = TRUE)
sum(no_review_growth$Apr2019QualifiedEntitlements, na.rm = TRUE) + sum(review_growth$Apr2019QualifiedEntitlements, na.rm = TRUE)

sum(no_review_nogrowth$Oct2019ActivatedEntitlements, na.rm = TRUE) + sum(review_nogrowth$Oct2019ActivatedEntitlements, na.rm = TRUE)
sum(no_review_nogrowth$Oct2019QualifiedEntitlements, na.rm = TRUE) + sum(review_nogrowth$Oct2019QualifiedEntitlements, na.rm = TRUE)

sum(no_review_nogrowth$Apr2019ActivatedEntitlements, na.rm = TRUE) + sum(review_nogrowth$Apr2019ActivatedEntitlements, na.rm = TRUE)
sum(no_review_nogrowth$Apr2019QualifiedEntitlements, na.rm = TRUE) + sum(review_nogrowth$Apr2019QualifiedEntitlements, na.rm = TRUE)

# 4) Control group usage
control <- inner_join(control, monthly_aeqe, by = "TPID")


# 4a) Growth 
control_growth <- filter(control, Recommendation %in% c(
  "This customer is expected to grow Teams adoption by 50%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" , 
  "This customer is expected to grow Teams adoption by 20%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption" ,        
  "This customer is expected to grow Teams adoption by 10%+ within the next year. Accelerate Teams usage and ensure no blockers to slow down adoption"))
sum(control_growth$Jul2019ActivatedEntitlements, na.rm = TRUE)/sum(control_growth$Jul2019QualifiedEntitlements, na.rm = TRUE)
sum(control_growth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(control_growth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(control_growth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(control_growth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(control_growth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(control_growth$Apr2019QualifiedEntitlements, na.rm = TRUE)

# 4b) No growth 
control_nogrowth <- filter(control, Recommendation %in% c("This customer's Teams potential is not expected to grow in the next year. Contact the customer and unblock Teams usage"))
sum(control_nogrowth$Jul2019ActivatedEntitlements, na.rm = TRUE)/sum(control_nogrowth$Jul2019QualifiedEntitlements, na.rm = TRUE)
sum(control_nogrowth$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(control_nogrowth$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(control_nogrowth$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(control_nogrowth$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(control_nogrowth$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(control_nogrowth$Apr2019QualifiedEntitlements, na.rm = TRUE)

# 5) Overall SMC segment 
smc <- filter(monthly_aeqe, `Summary Segment Name` %in% c("Small, Medium & Corporate Commercial", "Small, Medium & Corporate Public Sector"))
sum(smc$Jul2019ActivatedEntitlements, na.rm = TRUE)/sum(smc$Jul2019QualifiedEntitlements, na.rm = TRUE)
sum(smc$Oct2019ActivatedEntitlements, na.rm = TRUE)/sum(smc$Oct2019QualifiedEntitlements, na.rm = TRUE)
sum(smc$Feb2019ActivatedEntitlements, na.rm = TRUE)/sum(smc$Feb2019QualifiedEntitlements, na.rm = TRUE)
sum(smc$Apr2019ActivatedEntitlements, na.rm = TRUE)/sum(smc$Apr2019QualifiedEntitlements, na.rm = TRUE)




